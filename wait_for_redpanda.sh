#!/bin/sh
set -e

HOST="${1:-redpanda}"
PORT="${2:-9092}"

python3 - <<PY
import socket
import time

host = "$HOST"
port = int("$PORT")

for i in range(120):
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f"{host}:{port} est disponible")
            break
    except OSError:
        print(f"Attente de {host}:{port}...")
        time.sleep(2)
else:
    raise SystemExit("Redpanda indisponible")
PY