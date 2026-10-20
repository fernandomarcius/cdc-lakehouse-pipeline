#!/usr/bin/env python3
"""Worker: runs the sink in a loop and serves a tiny health endpoint.

  event-store  every CYCLE_INTERVAL seconds
  snapshot     every SNAPSHOT_INTERVAL seconds, in its own thread (a long snapshot must not delay event-store cycles)

Endpoints (PORT, default 8080):  /healthz (200 while the loop is alive, 503 when stuck)  and  /status (JSON).
SIGTERM/SIGINT stop the server cleanly. In a container the process is PID 1, which has no default action for SIGTERM:
without an explicit handler the signal is ignored and the orchestrator ends up sending SIGKILL.

Environment: CYCLE_INTERVAL (60), SNAPSHOT_INTERVAL (3600), PORT (8080), HEALTH_MAX_SECONDS (900), SINK_ACTIVE (true).
SINK_ACTIVE=false serves only the health endpoint. Standard library + sink/ only.
"""
import http.server
import json
import os
import signal
import socketserver
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sink"))

CYCLE_INTERVAL = int(os.environ.get("CYCLE_INTERVAL", "60"))
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "3600"))
PORT = int(os.environ.get("PORT", "8080"))
ACTIVE = os.environ.get("SINK_ACTIVE", "true").lower() == "true"
HEALTH_MAX = int(os.environ.get("HEALTH_MAX_SECONDS", "900"))

state = {"started": time.time(), "heartbeat": time.time(), "cycles": 0, "last": None, "consecutive_failures": 0}


def log(level, msg, **kv):
    print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "level": level, "logger": "cdc_worker", "message": msg, **kv},
                     ensure_ascii=False), flush=True)


def healthy(now=None):
    """Alive while the loop beat within HEALTH_MAX seconds (or when the sink is deliberately inactive)."""
    now = time.time() if now is None else now
    return (not ACTIVE) or now - state["heartbeat"] < HEALTH_MAX


def run_cycle(sink, cfg):
    """One event-store cycle. Never raises: an error becomes a log line and the next cycle tries again."""
    t0 = time.time()
    try:
        state["cycles"] += 1
        failures = sink.run_event_store(cfg)
        state["consecutive_failures"] = 0 if not failures else state["consecutive_failures"] + 1
        state["last"] = {"end": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "seconds": round(time.time() - t0, 1),
                         "failed_tables": failures, "lake_unavailable": sink.cycle_state["lake_unavailable"]}
    except Exception as e:
        state["consecutive_failures"] += 1
        log("ERROR", "cycle_failed", error=repr(e)[:500], consecutive_failures=state["consecutive_failures"])
    state["heartbeat"] = time.time()


def event_store_loop(sink, cfg, stop):
    while not stop.is_set():
        t0 = time.time()
        run_cycle(sink, cfg)
        stop.wait(max(0.0, CYCLE_INTERVAL - (time.time() - t0)))


def snapshot_loop(sink, cfg, stop):
    while not stop.is_set():
        try:
            sink.run_snapshot(cfg)
        except Exception as e:
            log("ERROR", "snapshot_failed", error=repr(e)[:500])
        stop.wait(SNAPSHOT_INTERVAL)


class Health(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="text/plain; charset=utf-8"):
        b = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/health", "/healthz"):
            ok = healthy()
            return self._send(200 if ok else 503, "ok\n" if ok else "loop stalled\n")
        if path == "/status":
            return self._send(200, json.dumps({**state, "active": ACTIVE}), "application/json")
        return self._send(404, "not found\n")


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    log("INFO", "starting", active=ACTIVE, interval=CYCLE_INTERVAL)
    stop = threading.Event()
    if ACTIVE:
        import sink
        cfg = sink.load_config()
        threading.Thread(target=event_store_loop, args=(sink, cfg, stop), daemon=True).start()
        if cfg["snapshot"]:
            threading.Thread(target=snapshot_loop, args=(sink, cfg, stop), daemon=True).start()
    server = Server(("0.0.0.0", PORT), Health)

    def terminate(*_):
        log("INFO", "shutting_down")
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    server.serve_forever()


if __name__ == "__main__":
    main()
    sys.exit(0)
