#!/bin/sh

echo "Attente de Redpanda..."

until rpk cluster info --brokers redpanda:9092 >/dev/null 2>&1
do
  echo "Redpanda pas encore prêt, nouvelle tentative..."
  sleep 2
done

echo "Redpanda prêt."

rpk topic create client_tickets --brokers redpanda:9092 || echo "Le topic client_tickets existe déjà."

echo "Initialisation terminée."