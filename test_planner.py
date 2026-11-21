import unittest

from planner import (
    AlertEvent,
    DedupPlanner,
    InhibitionRule,
    STATE_FIRING,
    STATE_INHIBITED,
    STATE_PENDING_RECOVERY,
    STATE_RESOLVED,
)

WINDOW = 300.0


def ev(source, fingerprint, service, state, ts, **labels):
    labels["service"] = service
    return AlertEvent(source=source, fingerprint=fingerprint,
                      labels=labels, state=state, timestamp=ts)


def only_group(report):
    assert len(report["groups"]) == 1, report["groups"]
    return report["groups"][0]


class MultiSourceDedupTest(unittest.TestCase):
    def test_same_fingerprint_from_three_sources_merges_to_one_group(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        planner.ingest([
            ev("metrics", "fp1", "api", "firing", 0),
            ev("logs", "fp1", "api", "firing", 5),
            ev("probe", "fp1", "api", "firing", 9),
            # Duplicate firings inside the window are still one incident.
            ev("metrics", "fp1", "api", "firing", 40),
        ])
        report = planner.evaluate(now=40)
        group = only_group(report)
        self.assertEqual(group["state"], STATE_FIRING)
        self.assertEqual(sorted(group["sources"]), ["logs", "metrics", "probe"])
        self.assertEqual(group["recurrence_count"], 0)
        self.assertEqual(len(group["episodes"]), 1)
        # Every duplicate is preserved as evidence with its origin source.
        self.assertEqual(len(group["evidence"]), 4)
        self.assertEqual(
            [e["source"] for e in group["evidence"]],
            ["metrics", "logs", "probe", "metrics"],
        )

    def test_similar_fingerprints_are_not_merged(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        planner.ingest([
            ev("metrics", "fp1", "api", "firing", 0),
            ev("metrics", "fp1", "api", "firing", 10),
            ev("metrics", "fp2", "api", "firing", 20),
        ])
        report = planner.evaluate(now=20)
        self.assertEqual(len(report["groups"]), 2)
        by_fp = {g["fingerprint"]: g for g in report["groups"]}
        self.assertEqual(len(by_fp["fp1"]["evidence"]), 2)
        self.assertEqual(len(by_fp["fp2"]["evidence"]), 1)

    def test_same_fingerprint_on_different_services_stays_separate(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        planner.ingest([
            ev("metrics", "fp1", "api", "firing", 0),
            ev("metrics", "fp1", "worker", "firing", 1),
        ])
        report = planner.evaluate(now=1)
        self.assertEqual(len(report["groups"]), 2)


class LabelChangeTest(unittest.TestCase):
    def test_non_identity_label_change_does_not_split_group(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        planner.ingest([
            ev("metrics", "fp1", "api", "firing", 0, version="v1", pod="p1"),
            ev("metrics", "fp1", "api", "firing", 10, version="v2", pod="p2"),
            ev("metrics", "fp1", "api", "resolved", 20, version="v2", pod="p2"),
        ])
        report = planner.evaluate(now=20)
        group = only_group(report)
        self.assertEqual(len(group["label_history"]), 2)
        self.assertEqual(group["label_history"][0]["labels"]["version"], "v1")
        self.assertEqual(group["label_history"][1]["labels"]["version"], "v2")
        self.assertEqual(len(group["episodes"]), 1)


class RecoveryTest(unittest.TestCase):
    def test_recovery_waits_for_all_sources_and_window(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        planner.ingest([
            ev("metrics", "fp1", "api", "firing", 0),
            ev("logs", "fp1", "api", "firing", 0),
            ev("metrics", "fp1", "api", "resolved", 100),
            # logs has not resolved yet: still firing.
        ])
        report = planner.evaluate(now=100)
        self.assertEqual(only_group(report)["state"], STATE_FIRING)

        planner.ingest([ev("logs", "fp1", "api", "resolved", 120)])
        # Inside the confirmation window: pending, not yet resolved.
        report = planner.evaluate(now=120 + WINDOW - 1)
        group = only_group(report)
        self.assertEqual(group["state"], STATE_PENDING_RECOVERY)
        rec = report["recovery"][group["key"]]
        self.assertEqual(rec["recovered_at"], 120)
        self.assertIsNone(rec["confirmed_at"])

        # Window elapsed with no new firing: recovery confirmed.
        report = planner.evaluate(now=120 + WINDOW)
        group = only_group(report)
        self.assertEqual(group["state"], STATE_RESOLVED)
        rec = report["recovery"][group["key"]]
        self.assertEqual(rec["confirmed_at"], 120 + WINDOW)
        self.assertTrue(all(e["recovered"] for e in rec["evidence"]))

    def test_out_of_order_events_reconcile_by_timestamp(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        # Delivered out of order: the early resolved from metrics must not
        # close the incident while logs is still firing afterwards.
        planner.ingest([
            ev("metrics", "fp1", "api", "resolved", 100),
            ev("logs", "fp1", "api", "firing", 150),
            ev("metrics", "fp1", "api", "firing", 0),
            ev("logs", "fp1", "api", "firing", 0),
        ])
        report = planner.evaluate(now=150)
        self.assertEqual(only_group(report)["state"], STATE_FIRING)

        planner.ingest([ev("logs", "fp1", "api", "resolved", 200)])
        report = planner.evaluate(now=200 + WINDOW)
        group = only_group(report)
        self.assertEqual(group["state"], STATE_RESOLVED)
        rec = report["recovery"][group["key"]]
        # Recovery is anchored at the last source's resolved timestamp.
        self.assertEqual(rec["recovered_at"], 200)

    def test_premature_recovery_reopened_by_late_firing(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        planner.ingest([
            ev("metrics", "fp1", "api", "firing", 0),
            ev("metrics", "fp1", "api", "resolved", 100),
            # Late firing inside the confirmation window cancels recovery.
            ev("metrics", "fp1", "api", "firing", 100 + WINDOW - 1),
        ])
        report = planner.evaluate(now=100 + WINDOW)
        group = only_group(report)
        self.assertEqual(group["state"], STATE_FIRING)
        self.assertEqual(len(group["episodes"]), 1)
        self.assertIsNone(group["episodes"][0]["recovered_at"])


class RecurrenceTest(unittest.TestCase):
    def test_flare_after_confirmed_recovery_is_a_recurrence(self):
        planner = DedupPlanner(window_seconds=WINDOW)
        planner.ingest([
            ev("metrics", "fp1", "api", "firing", 0),
            ev("metrics", "fp1", "api", "resolved", 100),
        ])
        report = planner.evaluate(now=100 + WINDOW)
        self.assertEqual(only_group(report)["state"], STATE_RESOLVED)

        # Quick flare-up after the confirmed recovery: same root cause
        # group, but a new episode counted as a recurrence.
        planner.ingest([ev("metrics", "fp1", "api", "firing", 100 + WINDOW + 30)])
        report = planner.evaluate(now=100 + WINDOW + 30)
        group = only_group(report)
        self.assertEqual(group["state"], STATE_FIRING)
        self.assertEqual(group["recurrence_count"], 1)
        self.assertEqual(len(group["episodes"]), 2)
        self.assertEqual(group["episodes"][0]["recovered_at"], 100)
        self.assertIsNone(group["episodes"][1]["recovered_at"])


class InhibitionTest(unittest.TestCase):
    def _planner(self):
        return DedupPlanner(
            window_seconds=WINDOW,
            inhibition_rules=[
                InhibitionRule(upstream_service="db",
                               downstream_service="api"),
            ],
            dependencies={"api": ["db"]},
        )

    def test_upstream_firing_inhibits_downstream(self):
        planner = self._planner()
        planner.ingest([
            ev("metrics", "fp-db", "db", "firing", 0),
            ev("metrics", "fp-api", "api", "firing", 10),
            ev("probe", "fp-api", "api", "firing", 12),
        ])
        report = planner.evaluate(now=12)
        by_svc = {g["service"]: g for g in report["groups"]}
        self.assertEqual(by_svc["db"]["state"], STATE_FIRING)
        # Downstream alert is recorded but inhibited, not actionable.
        self.assertEqual(by_svc["api"]["state"], STATE_INHIBITED)
        self.assertEqual(by_svc["api"]["inhibited_by"],
                         [by_svc["db"]["key"]])
        self.assertEqual(by_svc["api"]["depends_on"], ["db"])
        # Evidence is preserved even while inhibited.
        self.assertEqual(len(by_svc["api"]["evidence"]), 2)

        chains = report["inhibition_chains"]
        self.assertEqual(len(chains), 1)
        self.assertTrue(chains[0]["active"])
        self.assertEqual(chains[0]["upstream"], by_svc["db"]["key"])
        self.assertEqual(chains[0]["downstream"], by_svc["api"]["key"])

    def test_inhibition_release_makes_downstream_visible(self):
        planner = self._planner()
        planner.ingest([
            ev("metrics", "fp-db", "db", "firing", 0),
            ev("metrics", "fp-api", "api", "firing", 10),
        ])
        report = planner.evaluate(now=10)
        by_svc = {g["service"]: g for g in report["groups"]}
        self.assertEqual(by_svc["api"]["state"], STATE_INHIBITED)

        planner.ingest([ev("metrics", "fp-db", "db", "resolved", 50)])
        report = planner.evaluate(now=50 + WINDOW)
        by_svc = {g["service"]: g for g in report["groups"]}
        self.assertEqual(by_svc["db"]["state"], STATE_RESOLVED)
        # Inhibition released: the still-firing downstream alert is visible.
        self.assertEqual(by_svc["api"]["state"], STATE_FIRING)
        self.assertEqual(by_svc["api"]["inhibited_by"], [])

        chains = report["inhibition_chains"]
        self.assertEqual(len(chains), 1)
        self.assertFalse(chains[0]["active"])

    def test_inhibition_rule_label_matching(self):
        planner = DedupPlanner(
            window_seconds=WINDOW,
            inhibition_rules=[
                InhibitionRule(upstream_service="db",
                               downstream_service="api",
                               match_labels={"severity": "critical"}),
            ],
        )
        planner.ingest([
            ev("metrics", "fp-db", "db", "firing", 0),
            ev("metrics", "fp-api", "api", "firing", 10, severity="warning"),
        ])
        report = planner.evaluate(now=10)
        by_svc = {g["service"]: g for g in report["groups"]}
        # Rule only matches critical downstream alerts: not inhibited.
        self.assertEqual(by_svc["api"]["state"], STATE_FIRING)


class InputValidationTest(unittest.TestCase):
    def test_invalid_state_rejected(self):
        with self.assertRaises(ValueError):
            AlertEvent(source="metrics", fingerprint="fp",
                       labels={}, state="unknown", timestamp=0)

    def test_invalid_window_rejected(self):
        with self.assertRaises(ValueError):
            DedupPlanner(window_seconds=0)


if __name__ == "__main__":
    unittest.main()
