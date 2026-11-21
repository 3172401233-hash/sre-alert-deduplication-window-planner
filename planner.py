"""Offline alert deduplication and recovery planner.

The planner consumes a finite, offline batch of alert state-change events
(firing / resolved) coming from multiple sources (metrics, logs, synthetic
probes). It merges alerts that share the same root cause into groups,
tracks inhibition chains between dependent services, and decides when an
incident may be considered recovered.

It never talks to a live alerting system; it only consumes offline events.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

FIRING = "firing"
RESOLVED = "resolved"

STATE_FIRING = "firing"
STATE_INHIBITED = "inhibited"
STATE_PENDING_RECOVERY = "pending_recovery"
STATE_RESOLVED = "resolved"

_VALID_STATES = (FIRING, RESOLVED)


@dataclass
class AlertEvent:
    """A single state change reported by one alert source."""

    source: str
    fingerprint: str
    labels: Dict[str, str]
    state: str
    timestamp: float

    def __post_init__(self):
        if self.state not in _VALID_STATES:
            raise ValueError("unknown alert state: %r" % (self.state,))


@dataclass
class InhibitionRule:
    """While ``upstream_service`` has a firing alert, matching alerts on
    ``downstream_service`` are inhibited (recorded, but not actionable)."""

    upstream_service: str
    downstream_service: str
    match_labels: Dict[str, str] = field(default_factory=dict)

    def matches_downstream(self, labels):
        return all(labels.get(k) == v for k, v in self.match_labels.items())


@dataclass
class Episode:
    """One firing episode of a group. A group gains a new episode when it
    flares up again after a confirmed recovery (recurrence)."""

    started_at: float
    last_firing_at: float
    recovered_at: Optional[float] = None


class _SourceTrack:
    __slots__ = ("last_firing", "last_resolved")

    def __init__(self):
        self.last_firing = None  # type: Optional[float]
        self.last_resolved = None  # type: Optional[float]

    @property
    def active(self):
        if self.last_firing is None:
            return False
        return self.last_resolved is None or self.last_resolved < self.last_firing


class AlertGroup:
    """All events believed to share one root cause."""

    def __init__(self, fingerprint, identity):
        self.fingerprint = fingerprint
        self.identity = identity  # identity labels, e.g. {"service": "api"}
        self.sources = {}  # type: Dict[str, _SourceTrack]
        self.events = []  # type: List[AlertEvent]  full evidence, arrival order
        self.label_history = []  # type: List[Tuple[float, Dict[str, str]]]
        self.episodes = []  # type: List[Episode]
        self.recurrence_count = 0
        self._last_labels = None  # type: Optional[Dict[str, str]]

    @property
    def service(self):
        return self.identity.get("service")

    @property
    def labels(self):
        return dict(self._last_labels or self.identity)

    def active_sources(self):
        return sorted(s for s, t in self.sources.items() if t.active)

    def recovery_timestamp(self):
        """Earliest time at which every source that fired has resolved.

        Returns None while any source is still active. Because a resolved
        event may predate another source's last firing, recovery is keyed
        per source: a source only counts as recovered once its resolved
        timestamp is at or after its own last firing.
        """
        resolved = []
        for track in self.sources.values():
            if track.last_firing is None:
                continue
            if track.active:
                return None
            resolved.append(track.last_resolved)
        if not resolved:
            return None
        return max(resolved)


class DedupPlanner:
    """Merges multi-source alert events and plans recovery.

    Parameters
    ----------
    window_seconds:
        Length of the dedup / recovery-confirmation window. A recovery is
        only confirmed once the window has elapsed past the last source's
        resolved event with no new firing.
    inhibition_rules:
        Iterable of :class:`InhibitionRule`.
    dependencies:
        Mapping of downstream service -> list of upstream services. Used
        to annotate groups and inhibition chains; it never merges groups.
    identity_labels:
        Label names that, together with the fingerprint, identify a root
        cause. Changes to any *other* label are recorded as label history
        instead of splitting the group.
    """

    def __init__(self, window_seconds, inhibition_rules=(), dependencies=None,
                 identity_labels=("service",)):
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window = float(window_seconds)
        self.rules = list(inhibition_rules)
        self.dependencies = {k: list(v) for k, v in (dependencies or {}).items()}
        self.identity_labels = tuple(identity_labels)
        self._groups = {}  # type: Dict[Tuple, AlertGroup]
        self._max_ts = None  # type: Optional[float]

    def ingest(self, events):
        """Consume a batch of events. Events may arrive out of order; they
        are applied in timestamp order so late firings and early resolves
        reconcile correctly."""
        events = list(events)
        for ev in events:
            if not isinstance(ev, AlertEvent):
                raise TypeError("expected AlertEvent, got %r" % (ev,))
        ordered = sorted(enumerate(events),
                         key=lambda pair: (pair[1].timestamp, pair[0]))
        for _, ev in ordered:
            self._apply(ev)
            if self._max_ts is None or ev.timestamp > self._max_ts:
                self._max_ts = ev.timestamp

    def _group_key(self, ev):
        identity = tuple((name, ev.labels.get(name))
                         for name in self.identity_labels)
        return (ev.fingerprint, identity)

    def _apply(self, ev):
        key = self._group_key(ev)
        group = self._groups.get(key)
        if group is None:
            group = AlertGroup(ev.fingerprint, dict(key[1]))
            self._groups[key] = group
        group.events.append(ev)

        labels = dict(ev.labels)
        if group._last_labels is None:
            group.label_history.append((ev.timestamp, labels))
        elif labels != group._last_labels:
            group.label_history.append((ev.timestamp, labels))
        group._last_labels = labels

        track = group.sources.get(ev.source)
        if track is None:
            track = _SourceTrack()
            group.sources[ev.source] = track

        if ev.state == FIRING:
            if track.last_firing is None or ev.timestamp > track.last_firing:
                track.last_firing = ev.timestamp
            self._note_firing(group, ev.timestamp)
        else:
            if track.last_resolved is None or ev.timestamp > track.last_resolved:
                track.last_resolved = ev.timestamp
            self._note_resolved(group)

    def _note_firing(self, group, ts):
        episode = group.episodes[-1] if group.episodes else None
        if episode is None:
            group.episodes.append(Episode(started_at=ts, last_firing_at=ts))
            return
        if episode.recovered_at is not None:
            if ts >= episode.recovered_at + self.window:
                # Recovery had already been confirmed: this is a recurrence,
                # not a duplicate. Keep the old episode as evidence.
                group.recurrence_count += 1
                group.episodes.append(Episode(started_at=ts, last_firing_at=ts))
                return
            # Firing landed inside the confirmation window: the recovery was
            # premature (e.g. an early resolved event). Reopen the episode.
            episode.recovered_at = None
        if ts > episode.last_firing_at:
            episode.last_firing_at = ts

    def _note_resolved(self, group):
        if not group.episodes:
            return
        recovery_ts = group.recovery_timestamp()
        if recovery_ts is not None:
            group.episodes[-1].recovered_at = recovery_ts

    def evaluate(self, now=None):
        """Build a report of groups, states, inhibition chains and recovery
        evidence as of ``now`` (defaults to the newest event timestamp)."""
        if now is None:
            now = self._max_ts if self._max_ts is not None else 0.0

        base_states = {key: self._base_state(group, now)
                       for key, group in self._groups.items()}
        inhibited_by = self._compute_inhibition(base_states)

        groups_out = []
        recovery_out = {}
        for key in sorted(self._groups, key=str):
            group = self._groups[key]
            base = base_states[key]
            blockers = inhibited_by.get(key, [])
            state = STATE_INHIBITED if (base == STATE_FIRING and blockers) else base
            group_key = self._key_str(key)
            groups_out.append(self._group_report(
                group_key, group, state, blockers, now))
            recovery_out[group_key] = self._recovery_report(group, base, now)

        return {
            "generated_at": now,
            "window_seconds": self.window,
            "groups": groups_out,
            "inhibition_chains": self._chain_report(base_states),
            "recovery": recovery_out,
        }

    def _base_state(self, group, now):
        if group.active_sources():
            return STATE_FIRING
        recovery_ts = group.recovery_timestamp()
        if recovery_ts is None:
            return STATE_FIRING
        if now >= recovery_ts + self.window:
            return STATE_RESOLVED
        return STATE_PENDING_RECOVERY

    def _compute_inhibition(self, base_states):
        firing = {key for key, state in base_states.items()
                  if state == STATE_FIRING}
        inhibited_by = {}
        for key in firing:
            group = self._groups[key]
            for rule in self.rules:
                if group.service != rule.downstream_service:
                    continue
                if not rule.matches_downstream(group.labels):
                    continue
                for up_key in firing:
                    if up_key == key:
                        continue
                    if self._groups[up_key].service == rule.upstream_service:
                        inhibited_by.setdefault(key, []).append(
                            self._key_str(up_key))
        return inhibited_by

    def _chain_report(self, base_states):
        chains = []
        for rule in self.rules:
            ups = [k for k, g in self._groups.items()
                   if g.service == rule.upstream_service]
            downs = [k for k, g in self._groups.items()
                     if g.service == rule.downstream_service
                     and rule.matches_downstream(g.labels)]
            for up_key in sorted(ups, key=str):
                for down_key in sorted(downs, key=str):
                    active = (base_states[up_key] == STATE_FIRING
                              and base_states[down_key] == STATE_FIRING)
                    chains.append({
                        "rule": {
                            "upstream_service": rule.upstream_service,
                            "downstream_service": rule.downstream_service,
                            "match_labels": dict(rule.match_labels),
                        },
                        "upstream": self._key_str(up_key),
                        "downstream": self._key_str(down_key),
                        # active == inhibiting right now; False == released.
                        "active": active,
                    })
        return chains

    def _group_report(self, group_key, group, state, inhibited_by, now):
        return {
            "key": group_key,
            "fingerprint": group.fingerprint,
            "service": group.service,
            "state": state,
            "inhibited_by": sorted(inhibited_by),
            "depends_on": list(self.dependencies.get(group.service, ())),
            "recurrence_count": group.recurrence_count,
            "episodes": [
                {
                    "started_at": ep.started_at,
                    "last_firing_at": ep.last_firing_at,
                    "recovered_at": ep.recovered_at,
                }
                for ep in group.episodes
            ],
            "sources": {
                source: {
                    "last_firing": track.last_firing,
                    "last_resolved": track.last_resolved,
                    "active": track.active,
                }
                for source, track in sorted(group.sources.items())
            },
            "label_history": [
                {"timestamp": ts, "labels": dict(labels)}
                for ts, labels in group.label_history
            ],
            "evidence": [
                {
                    "source": ev.source,
                    "state": ev.state,
                    "timestamp": ev.timestamp,
                    "labels": dict(ev.labels),
                }
                for ev in group.events
            ],
        }

    def _recovery_report(self, group, base_state, now):
        recovery_ts = group.recovery_timestamp()
        confirmed_at = None
        if recovery_ts is not None and now >= recovery_ts + self.window:
            confirmed_at = recovery_ts + self.window
        return {
            "state": base_state,
            "recovered_at": recovery_ts,
            "confirmed_at": confirmed_at,
            "evidence": [
                {
                    "source": source,
                    "last_firing": track.last_firing,
                    "last_resolved": track.last_resolved,
                    "recovered": (track.last_firing is not None
                                  and not track.active),
                }
                for source, track in sorted(group.sources.items())
            ],
        }

    @staticmethod
    def _key_str(key):
        fingerprint, identity = key
        parts = ["%s=%s" % (name, value) for name, value in identity]
        return "%s|%s" % (fingerprint, ",".join(parts))
