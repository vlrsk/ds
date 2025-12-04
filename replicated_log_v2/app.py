import logging
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, request, jsonify


ROLE = os.getenv("ROLE", "master").lower()
PORT = int(os.getenv("PORT", "5000"))


SECONDARIES = [
    s.strip() for s in os.getenv("SECONDARIES", "").split(",") if s.strip()
]

REPLICATION_DELAY = float(os.getenv("REPLICATION_DELAY", "0"))
HTTP_REPL_TIMEOUT = float(os.getenv("HTTP_REPL_TIMEOUT", "30"))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger(ROLE)

app = Flask(__name__)


messages = {}
seen_message_ids = set()

messages_lock = threading.Lock()
id_lock = threading.Lock()

next_message_id = 0


def allocate_message_id() -> int:
    global next_message_id
    with id_lock:
        mid = next_message_id
        next_message_id += 1
    return mid


def store_message(message_id: int, text: str):
    with messages_lock:
        if message_id in seen_message_ids:
            current_size = len(messages)
            return False, current_size

        seen_message_ids.add(message_id)
        messages[message_id] = text
        current_size = len(messages)
    return True, current_size


def get_ordered_messages():
    with messages_lock:
        ordered_ids = sorted(messages.keys())
        return [messages[mid] for mid in ordered_ids]


def register_master_routes():
    @app.route("/messages", methods=["GET"])
    def get_messages_master():
        current_messages = get_ordered_messages()
        logger.info("GET /messages (master) -> %d messages", len(current_messages))
        return jsonify({"messages": current_messages})

    @app.route("/messages", methods=["POST"])
    def post_message():
        data = request.get_json()
        if not data or "message" not in data:
            return (
                jsonify({"error": "JSON body with 'message' and 'w' fields is required"}),
                400,
            )

        if "w" not in data:
            return jsonify({"error": "Write concern 'w' is required"}), 400

        msg = str(data["message"])
        try:
            w = int(data["w"])
        except (TypeError, ValueError):
            return jsonify({"error": "Write concern 'w' must be an integer"}), 400

        total_nodes = 1 + len(SECONDARIES)
        if w < 1 or w > total_nodes:
            return (
                jsonify(
                    {
                        "error": "Invalid write concern 'w'",
                        "details": f"w must be between 1 and {total_nodes}",
                    }
                ),
                400,
            )

        logger.info("Received new message on master: %r with w=%d", msg, w)

        msg_id = allocate_message_id()
        is_new, size_now = store_message(msg_id, msg)
        if is_new:
            logger.info("Master log size is now %d", size_now)
        else:
            logger.info("Duplicate message id=%d on master, ignored", msg_id)

        if not SECONDARIES:
            return jsonify({"status": "ok", "id": msg_id, "message": msg}), 201

        errors = [None] * len(SECONDARIES)

        required_acks_from_secondaries = max(0, w - 1)

        def replicate_to_secondary(index, secondary_base_url):
            replicate_url = f"{secondary_base_url.rstrip('/')}/replicate"
            logger.info("Replicating to %s ...", replicate_url)
            try:
                resp = requests.post(
                    replicate_url,
                    json={"id": msg_id, "message": msg},
                    timeout=HTTP_REPL_TIMEOUT,
                )
                resp.raise_for_status()
                logger.info("ACK received from %s", secondary_base_url)
                return True
            except Exception as e:
                logger.error("Replication to %s failed: %s", secondary_base_url, e)
                errors[index] = e
                return False

        futures = []
        executor = ThreadPoolExecutor(max_workers=len(SECONDARIES))

        for i, secondary_base_url in enumerate(SECONDARIES):
            futures.append(executor.submit(replicate_to_secondary, i, secondary_base_url))

        try:
            if required_acks_from_secondaries == 0:
                logger.info("w=1 -> not waiting for secondary ACKs")
                return jsonify({"status": "ok", "id": msg_id, "message": msg}), 201

            for fut in as_completed(futures):
                ok = fut.result()
                if ok:
                    required_acks_from_secondaries -= 1

                if required_acks_from_secondaries == 0:
                    return jsonify({"status": "ok", "id": msg_id, "message": msg}), 201

            successful_acks = (w - 1) - required_acks_from_secondaries
            logger.error("Failed to satisfy write concern w=%d for id=%d: got only %d ACKs from secondaries",w, msg_id, successful_acks)
            return jsonify({"error": "Failed to satisfy write concern", "w": w, "acks_from_secondaries": successful_acks,}),500
        finally:
            executor.shutdown(wait=False)


def register_secondary_routes():
    @app.route("/messages", methods=["GET"])
    def get_messages_secondary():
        current_messages = get_ordered_messages()
        logger.info(
            "GET /messages (secondary) -> %d messages", len(current_messages)
        )
        return jsonify({"messages": current_messages})

    @app.route("/replicate", methods=["POST"])
    def replicate():
        data = request.get_json()
        if not data or "message" not in data or "id" not in data:
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
            msg_id = int(data["id"])
        except (TypeError, ValueError):
            return jsonify({"error": "'id' must be an integer"}), 400
        logger.info(
            "Replication request received with id=%d, message: %r", msg_id, msg
        )
        if REPLICATION_DELAY > 0:
            logger.info(
                "Simulating replication delay: %.2f seconds", REPLICATION_DELAY
            )
            time.sleep(REPLICATION_DELAY)
        is_new, size_now = store_message(msg_id, msg)
        if is_new:
            logger.info("Secondary log size is now %d", size_now)
        else:
            logger.info(
                "Duplicate message id=%d on secondary, ignored (dedup)", msg_id
            )
        return jsonify({"status": "ack"}), 200


if ROLE == "master":
    logger.info("Starting in MASTER mode on port %d", PORT)
    if not SECONDARIES:
        logger.warning(
            "No SECONDARIES configured! Set env SECONDARIES, e.g. "
            "'http://secondary1:5001,http://secondary2:5002'"
        )
    register_master_routes()
else:
    logger.info("Starting in SECONDARY mode on port %d", PORT)
    register_secondary_routes()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)