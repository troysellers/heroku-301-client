#!/usr/bin/env bash

set -eo pipefail

function cleanup {
  rm -f client.crt client.key
  echo ""
  echo "-----> Exiting."
  exit
}
trap cleanup SIGINT SIGTERM

if [[ -z "${HEROKU_KAFKA_PURPLE_CLIENT_CERT}" ]]; then
    echo "KAFKA_CLIENT_CERT is not set. Aborting"
    exit 1
fi

if [[ -z "${HEROKU_KAFKA_PURPLE_CLIENT_CERT_KEY}" ]]; then
    echo "KAFKA_CLIENT_CERT_KEY is not set. Aborting"
    exit 1
fi

if [[ -z "${HEROKU_KAFKA_PURPLE_URL}" ]]; then
    echo "KAFKA_URL is not set. Aborting"
    exit 1
fi

# Setup cert and cert key
rm -f client.crt client.key
echo -n "$HEROKU_KAFKA_PURPLE_CLIENT_CERT" > client.crt
echo -n "$HEROKU_KAFKA_PURPLE_CLIENT_CERT_KEY" > client.key