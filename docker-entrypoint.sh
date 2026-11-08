#!/bin/sh
# Sob o agente New Relic quando a licenca existe; direto caso contrario.
set -e

if [ -n "$NEW_RELIC_LICENSE_KEY" ]; then
    echo "INFO: subindo sob o agente New Relic"
    exec newrelic-admin run-program "$@"
fi

exec "$@"
