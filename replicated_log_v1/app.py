import logging
import os
import time
import threading

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

messages = []
messages_lock = threading.Lock()

app = Flask(__name__)


def register_master_routes():
    @app.route("/messages", methods=["GET"])
    def get_messages():
        with messages_lock:
            current_messages = list(messages)
        logger.info("GET /messages (master) -> %d messages", len(current_messages))
        return jsonify({"messages": current_messages})

    @app.route("/messages", methods=["POST"])
    def post_message():
        data = request.get_json()
        if not data or "message" not in data:
            return jsonify({"error": "JSON body with 'message' field is required"}), 400

        msg = str(data["message"])
        logger.info("Received new message on master: %r", msg)

        with messages_lock:
            messages.append(msg)
            logger.info("Master log size is now %d", len(messages))

        if not SECONDARIES:
            return jsonify({"status": "ok", "message": msg}), 201

        errors = [None] * len(SECONDARIES)
        threads = []

        def replicate_to_secondary(index, secondary_base_url):
            replicate_url = f"{secondary_base_url.rstrip('/')}/replicate"
            logger.info("Replicating to %s ...", replicate_url)
            try:
                resp = requests.post(
                    replicate_url,
                    json={"message": msg},
                    timeout=HTTP_REPL_TIMEOUT,
                )
                resp.raise_for_status()
                logger.info("ACK received from %s", secondary_base_url)
            except Exception as e:
                logger.error("Replication to %s failed: %s", secondary_base_url, e)
                errors[index] = e

        for i, secondary_base_url in enumerate(SECONDARIES):
            t = threading.Thread(
                target=replicate_to_secondary,
                args=(i, secondary_base_url),
                daemon=True,
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        for idx, err in enumerate(errors):
            if err is not None:
                return jsonify(
                    {
                        "error": "Failed to replicate to secondary",
                        "secondary": SECONDARIES[idx],
                        "details": str(err),
                    }
                ), 500
        return jsonify({"status": "ok", "message": msg}), 201


def register_secondary_routes():
    @app.route("/messages", methods=["GET"])
    def get_messages_secondary():
        with messages_lock:
            current_messages = list(messages)
        logger.info("GET /messages (secondary) -> %d messages", len(current_messages))
        return jsonify({"messages": current_messages})

    @app.route("/replicate", methods=["POST"])
    def replicate():
        data = request.get_json()
        if not data or "message" not in data:
            return jsonify({"error": "JSON body with 'message' field is required"}), 400

        msg = str(data["message"])
        logger.info("Replication request received with message: %r", msg)

        if REPLICATION_DELAY > 0:
            logger.info("Simulating replication delay: %.2f seconds", REPLICATION_DELAY)
            time.sleep(REPLICATION_DELAY)

        with messages_lock:
            messages.append(msg)
            logger.info("Secondary log size is now %d", len(messages))

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