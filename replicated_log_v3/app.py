import logging
import os
import time
import threading
import random

from flask import Flask, request, jsonify

ROLE = os.getenv("ROLE", "master").lower()
PORT = int(os.getenv("PORT", "5000"))

SECONDARIES = [
    s.strip() for s in os.getenv("SECONDARIES", "").split(",") if s.strip()
]

REPLICATION_DELAY = float(os.getenv("REPLICATION_DELAY", "0"))
HTTP_REPL_TIMEOUT = float(os.getenv("HTTP_REPL_TIMEOUT", "30"))
INTERNAL_ERROR_PROB = float(os.getenv("INTERNAL_ERROR_PROB", "0.0"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "5.0"))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "2.0"))
SUSPECT_TIMEOUT = float(os.getenv("SUSPECT_TIMEOUT", "10.0"))
UNHEALTHY_TIMEOUT = float(os.getenv("UNHEALTHY_TIMEOUT", "30.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger(ROLE)

event_log = {}
log_lock = threading.Lock()
next_event_id = 0

app = Flask(__name__)


def get_visible_messages():
    with log_lock:
        if not event_log:
            return []

        ids = sorted(event_log.keys())
        visible_ids = []

        expected = ids[0]
        for eid in ids:
            if eid != expected:
                break
            visible_ids.append(eid)
            expected += 1

        return [event_log[eid]["message"] for eid in visible_ids]


def get_max_event_id():
    with log_lock:
        if not event_log:
            return 0
        return max(event_log.keys())


def append_event_master(msg: str):
    global next_event_id
    with log_lock:
        next_event_id += 1
        eid = next_event_id
        event = {"id": eid, "message": msg}
        event_log[eid] = event
        logger.info("Master event log size is now %d", len(event_log))
    return event


def apply_event_on_secondary(eid: int, msg: str):
    with log_lock:
        if eid in event_log:
            logger.info("Duplicate event id %d on secondary, ignoring", eid)
            return
        event_log[eid] = {"id": eid, "message": msg}
        logger.info("Secondary event log size is now %d", len(event_log))


secondary_states = {}
secondary_states_lock = threading.Lock()
master_read_only = False
write_concern_lock = threading.Lock()
write_concern_state = {}


def init_secondary_states():
    if ROLE != "master":
        return
    now = time.time()
    with secondary_states_lock:
        for url in SECONDARIES:
            secondary_states[url] = {
                "status": "healthy",
                "last_heartbeat": now,
                "last_acked_id": 0,
            }


def quorum_size():
    cluster_size = 1 + len(SECONDARIES)
    return cluster_size // 2 + 1


def evaluate_read_only():
    global master_read_only
    if ROLE != "master":
        return
    with secondary_states_lock:
        healthy_secondaries = sum(
            1 for st in secondary_states.values() if st["status"] == "healthy"
        )
        healthy_nodes = 1 + healthy_secondaries
        master_read_only = healthy_nodes < quorum_size()


def is_master_read_only():
    if ROLE != "master":
        return False
    with secondary_states_lock:
        return master_read_only


def note_secondary_acked_range(secondary_url: str, from_eid: int, to_eid: int):
    if to_eid <= from_eid:
        return
    with write_concern_lock:
        for eid in range(from_eid + 1, to_eid + 1):
            st = write_concern_state.get(eid)
            if not st:
                continue
            if secondary_url in st["acked"]:
                continue
            st["acked"].add(secondary_url)
            if len(st["acked"]) >= st["needed"]:
                st["cond"].notify_all()


def heartbeat_worker():
    if ROLE != "master" or not SECONDARIES:
        return

    import requests

    while True:
        now = time.time()
        with secondary_states_lock:
            urls = list(secondary_states.keys())

        for url in urls:
            prev_last_acked = None
            new_last_acked = None
            try:
                health_url = f"{url.rstrip('/')}/health"
                logger.debug("Heartbeat -> %s", health_url)
                resp = requests.get(health_url, timeout=HEARTBEAT_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    last_id = int(data.get("last_applied_id", 0))
                    with secondary_states_lock:
                        st = secondary_states.get(url)
                        if st:
                            prev_last_acked = st["last_acked_id"]
                            st["status"] = "healthy"
                            st["last_heartbeat"] = now
                            if last_id > st["last_acked_id"]:
                                st["last_acked_id"] = last_id
                            new_last_acked = st["last_acked_id"]

                    if prev_last_acked is not None and new_last_acked is not None:
                        note_secondary_acked_range(
                            url, prev_last_acked, new_last_acked
                        )
                    continue
            except Exception as e:
                logger.warning("Heartbeat to %s failed: %s", url, e)

            with secondary_states_lock:
                st = secondary_states.get(url)
                if not st:
                    continue
                last_ok = st.get("last_heartbeat") or 0
                dt = now - last_ok
                if dt > UNHEALTHY_TIMEOUT:
                    st["status"] = "unhealthy"
                elif dt > SUSPECT_TIMEOUT:
                    st["status"] = "suspected"

        evaluate_read_only()
        time.sleep(HEARTBEAT_INTERVAL)


def register_write_concern(event_id: int, needed_from_secondaries: int):
    if needed_from_secondaries <= 0:
        return
    with write_concern_lock:
        if event_id not in write_concern_state:
            cond = threading.Condition(write_concern_lock)
            write_concern_state[event_id] = {
                "needed": needed_from_secondaries,
                "acked": set(),
                "cond": cond,
            }
        else:
            st = write_concern_state[event_id]
            st["needed"] = max(st["needed"], needed_from_secondaries)


def wait_for_write_concern(event_id: int, needed_from_secondaries: int):
    if needed_from_secondaries <= 0:
        return
    with write_concern_lock:
        st = write_concern_state.get(event_id)
        if not st:
            return
        cond = st["cond"]
        while len(st["acked"]) < needed_from_secondaries:
            cond.wait()


def note_secondary_acked(secondary_url: str, event_id: int):
    with secondary_states_lock:
        st = secondary_states.get(secondary_url)
        if st:
            st["last_acked_id"] = max(st["last_acked_id"], event_id)

    with write_concern_lock:
        st = write_concern_state.get(event_id)
        if not st:
            return
        if secondary_url in st["acked"]:
            return
        st["acked"].add(secondary_url)
        if len(st["acked"]) >= st["needed"]:
            st["cond"].notify_all()


def replication_worker(secondary_url: str):
    if ROLE != "master":
        return

    import requests

    logger.info("Starting replication worker for %s", secondary_url)

    while True:
        with secondary_states_lock:
            st = secondary_states.get(secondary_url)
            last_acked = st["last_acked_id"] if st else 0

        next_id = last_acked + 1

        with log_lock:
            if event_log:
                max_id = max(event_log.keys())
            else:
                max_id = 0
            event = event_log.get(next_id)

        if next_id > 0 and event and next_id <= max_id:
            replicate_url = f"{secondary_url.rstrip('/')}/replicate"
            try:
                logger.info(
                    "Replicating event %s to %s ...", event["id"], replicate_url
                )
                resp = requests.post(
                    replicate_url, json=event, timeout=HTTP_REPL_TIMEOUT
                )
                resp.raise_for_status()
                logger.info(
                    "Replication of event %s to %s succeeded",
                    event["id"],
                    secondary_url,
                )
                note_secondary_acked(secondary_url, event["id"])
                continue
            except Exception as e:
                logger.error(
                    "Replication of event %s to %s failed: %s",
                    event["id"],
                    secondary_url,
                    e,
                )
                time.sleep(1.0)
                continue

        time.sleep(0.2)


def start_master_background_workers():
    if ROLE != "master" or not SECONDARIES:
        return

    init_secondary_states()

    t = threading.Thread(target=heartbeat_worker, daemon=True)
    t.start()

    for url in SECONDARIES:
        threading.Thread(
            target=replication_worker, args=(url,), daemon=True
        ).start()


def register_master_routes():
    @app.route("/messages", methods=["GET"])
    def get_messages():
        msgs = get_visible_messages()
        logger.info("GET /messages (master) -> %d visible messages", len(msgs))
        return jsonify({"messages": msgs})

    @app.route("/messages", methods=["POST"])
    def post_message():
        if is_master_read_only():
            return (
                jsonify(
                    {
                        "error": "master in read-only mode due to lack of quorum",
                        "quorum_size": quorum_size(),
                    }
                ),
                503,
            )

        data = request.get_json() or {}
        if "message" not in data:
            return jsonify({"error": "JSON body with 'message' field is required"}), 400

        msg = str(data["message"])
        w = int(data.get("w", 1))

        cluster_size = 1 + len(SECONDARIES)
        if w < 1 or w > cluster_size:
            return (
                jsonify(
                    {
                        "error": f"Invalid write concern w={w}. "
                                 f"Must be between 1 and {cluster_size}",
                        "cluster_size": cluster_size,
                    }
                ),
                400,
            )

        logger.info(
            "Received new message on master: %r with write concern w=%d", msg, w
        )
        event = append_event_master(msg)
        needed_from_secondaries = max(0, w - 1)
        register_write_concern(event["id"], needed_from_secondaries)
        if needed_from_secondaries > 0:
            wait_for_write_concern(event["id"], needed_from_secondaries)

        return (
            jsonify(
                {
                    "status": "ok",
                    "message": msg,
                    "id": event["id"],
                    "w": w,
                    "acked_from_secondaries": needed_from_secondaries,
                }
            ),
            201,
        )

    @app.route("/health", methods=["GET"])
    def health_master():
        with secondary_states_lock:
            secondaries_info = []
            for url, st in secondary_states.items():
                secondaries_info.append(
                    {
                        "url": url,
                        "status": st["status"],
                        "last_heartbeat": st["last_heartbeat"],
                        "last_acked_id": st["last_acked_id"],
                    }
                )
            readonly = master_read_only

        return jsonify(
            {
                "role": "master",
                "read_only": readonly,
                "quorum_size": quorum_size(),
                "cluster_size": 1 + len(SECONDARIES),
                "secondaries": secondaries_info,
            }
        )


def register_secondary_routes():
    @app.route("/messages", methods=["GET"])
    def get_messages_secondary():
        msgs = get_visible_messages()
        logger.info("GET /messages (secondary) -> %d visible messages", len(msgs))
        return jsonify({"messages": msgs})

    @app.route("/replicate", methods=["POST"])
    def replicate():
        data = request.get_json() or {}
        if "message" not in data or "id" not in data:
            return (
                jsonify(
                    {
                        "error": "JSON body with 'id' and 'message' fields is required"
                    }
                ),
                400,
            )

        msg = str(data["message"])
        try:
            eid = int(data["id"])
        except (TypeError, ValueError):
            return jsonify({"error": "'id' must be an integer"}), 400
        logger.info(
            "Replication request received with id=%d, message=%r", eid, msg
        )
        if REPLICATION_DELAY > 0:
            logger.info(
                "Simulating replication delay: %.2f seconds", REPLICATION_DELAY
            )
            time.sleep(REPLICATION_DELAY)

        apply_event_on_secondary(eid, msg)

        if INTERNAL_ERROR_PROB > 0 and random.random() < INTERNAL_ERROR_PROB:
            logger.warning(
                "Simulating internal server error AFTER applying event id=%d", eid
            )
            return jsonify({"error": "simulated internal error"}), 500

        return jsonify({"status": "ack"}), 200

    @app.route("/health", methods=["GET"])
    def health_secondary():
        last_id = get_max_event_id()
        return jsonify(
            {
                "role": "secondary",
                "status": "ok",
                "last_applied_id": last_id,
            }
        )


if ROLE == "master":
    logger.info("Starting in MASTER mode on port %d", PORT)
    if not SECONDARIES:
        logger.warning(
            "No SECONDARIES configured! Set env SECONDARIES, e.g. "
            "'http://secondary1:5001,http://secondary2:5002'"
        )
    register_master_routes()
    start_master_background_workers()
else:
    logger.info("Starting in SECONDARY mode on port %d", PORT)
    register_secondary_routes()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)