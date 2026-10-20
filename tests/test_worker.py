import threading
import time

import cycle


class FakeSink:
    cycle_state = {"lake_unavailable": False}

    def __init__(self, failures=0, error=None):
        self.failures, self.error, self.runs = failures, error, 0

    def run_event_store(self, cfg):
        self.runs += 1
        if self.error:
            raise self.error
        return self.failures


def reset():
    cycle.state.update({"cycles": 0, "last": None, "consecutive_failures": 0, "heartbeat": time.time()})


def test_cycle_success_resets_failures():
    reset()
    cycle.state["consecutive_failures"] = 3
    cycle.run_cycle(FakeSink(), {})
    assert cycle.state["consecutive_failures"] == 0 and cycle.state["cycles"] == 1
    assert cycle.state["last"]["failed_tables"] == 0


def test_cycle_counts_table_failures():
    reset()
    cycle.run_cycle(FakeSink(failures=2), {})
    assert cycle.state["consecutive_failures"] == 1 and cycle.state["last"]["failed_tables"] == 2


def test_cycle_never_raises():
    reset()
    cycle.run_cycle(FakeSink(error=RuntimeError("boom")), {})
    assert cycle.state["consecutive_failures"] == 1


def test_health_depends_on_heartbeat(monkeypatch):
    monkeypatch.setattr(cycle, "ACTIVE", True)
    cycle.state["heartbeat"] = 1000.0
    assert cycle.healthy(now=1000.0 + cycle.HEALTH_MAX - 1)
    assert not cycle.healthy(now=1000.0 + cycle.HEALTH_MAX + 1)


def test_inactive_is_always_healthy(monkeypatch):
    monkeypatch.setattr(cycle, "ACTIVE", False)
    cycle.state["heartbeat"] = 0.0
    assert cycle.healthy()


def test_loop_stops_on_event(monkeypatch):
    reset()
    monkeypatch.setattr(cycle, "CYCLE_INTERVAL", 0)
    stop, fake = threading.Event(), FakeSink()
    t = threading.Thread(target=cycle.event_store_loop, args=(fake, {}, stop))
    t.start()
    time.sleep(0.05)
    stop.set()
    t.join(timeout=2)
    assert not t.is_alive() and fake.runs >= 1
