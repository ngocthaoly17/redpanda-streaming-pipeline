import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "redpanda:9092"
TOPIC_NAME = "client_tickets"

REQUESTS = [
    ("Je n'arrive pas à me connecter à mon compte", "technical"),
    ("Je souhaite modifier mon abonnement", "billing"),
    ("Mon paiement a été refusé", "billing"),
    ("L'application plante au démarrage", "bug"),
    ("Je veux réinitialiser mon mot de passe", "technical"),
    ("Je souhaite obtenir des informations sur un produit", "general"),
    ("Je veux fermer mon compte", "general"),
    ("Le site est très lent", "bug"),
]

PRIORITIES = ["low", "medium", "high"]


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


def generate_ticket(ticket_id: int) -> dict:
    request_text, request_type = random.choice(REQUESTS)

    return {
        "ticket_id": f"TKT-{ticket_id:06d}",
        "client_id": f"CL-{random.randint(1, 500):05d}",
        "created_at": datetime.utcnow().isoformat(),
        "request": request_text,
        "request_type": request_type,
        "priority": random.choice(PRIORITIES),
    }


def main() -> None:
    producer = build_producer()
    ticket_id = 1

    print(f"Envoi des tickets vers Redpanda sur {BOOTSTRAP_SERVERS}, topic={TOPIC_NAME}")

    try:
        while True:
            ticket = generate_ticket(ticket_id)
            producer.send(TOPIC_NAME, ticket)
            producer.flush()
            print(f"[OK] Ticket envoyé : {ticket}")
            ticket_id += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArrêt du producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
