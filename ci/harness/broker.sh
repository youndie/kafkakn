#!/usr/bin/env bash
# The broker, and the two third parties every assertion goes through.
#
# Assertions never read back through kafkakn itself: a producer verified by its own consumer can be
# wrong in both directions at once and agree with itself. The oracles here are the broker's own end
# offsets and kafka-console-consumer.
#
#   broker.sh up|down|topic <name>|offsets <name>|consume <name> <pattern>|selftest
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
COMPOSE="$HERE/../broker/docker-compose.yml"
TLS_COMPOSE="$HERE/../broker/docker-compose.tls.yml"
SSL_BOOTSTRAP=${SSL_BOOTSTRAP:-127.0.0.1:9094}
CONTAINER=kafkakn-broker
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PARTITIONS=${PARTITIONS:-3}

kc() { docker exec "$CONTAINER" "$@"; }
# -i belongs BEFORE the container name. `docker exec <name> -i` makes "-i" the command, and a
# producer piped into an exec with no stdin reads EOF, exits zero and writes nothing.
kci() { docker exec -i "$CONTAINER" "$@"; }

case "${1:-}" in
  up|tls-up)
    # ONE fixture with BOTH listeners, always. The alternative - a plaintext mode and a TLS mode -
    # buys a suite that skips its TLS scenarios in the mode that does not have them, and a skipped
    # scenario reads exactly like a passing one in a summary line.
    #
    # `--wait` is NOT trusted to mean the broker is up. Measured 2026-09-17: with a required
    # configuration key missing, the broker aborted in StorageTool, the container crash-looped with
    # exit 1, and compose reported it **Healthy** anyway. A fixture that reports success while its
    # subject is dead makes every test above it meaningless, so the only evidence accepted here is
    # the broker answering a request.
    bash "$HERE/../broker/certs.sh" || exit 1
    docker compose -f "$COMPOSE" -f "$TLS_COMPOSE" up -d --wait < /dev/null >/dev/null 2>&1
    answered=
    for _ in $(seq 1 30); do
        if kc /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" \
                >/dev/null 2>&1; then
            echo "  broker answers on $BOOTSTRAP"
            answered=yes
            break
        fi
        sleep 2
    done
    [ -n "$answered" ] || {
        echo "  BROKER DID NOT ANSWER on $BOOTSTRAP - here is why:" >&2
        docker logs "$CONTAINER" 2>&1 | grep -iE "exception|error|missing" | tail -3 >&2
        exit 1
    }
    for _ in $(seq 1 30); do
        if kc /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$SSL_BOOTSTRAP" \
                --command-config /etc/kafka/secrets/client-ssl.properties >/dev/null 2>&1; then
            echo "  broker answers over TLS on $SSL_BOOTSTRAP"
            exit 0
        fi
        sleep 2
    done
    echo "  BROKER DID NOT ANSWER OVER TLS on $SSL_BOOTSTRAP - here is why:" >&2
    docker logs "$CONTAINER" 2>&1 | grep -iE "ssl|exception|error" | tail -5 >&2
    exit 1
    ;;
  tls-selftest)
    # The fixture must be able to say NO. A broker that accepts every CA, or a client that never
    # verifies one, produces exactly the same green as a working TLS setup - and this is the one
    # question that tells them apart, asked with the broker's own tools rather than with kafkakn.
    kc /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$SSL_BOOTSTRAP" \
        --command-config /etc/kafka/secrets/client-ssl.properties >/dev/null 2>&1 \
        || { echo "tls-selftest: the RIGHT CA was refused - the fixture is broken" >&2; exit 1; }
    if kc /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$SSL_BOOTSTRAP" \
            --command-config /etc/kafka/secrets/client-ssl-wrong-ca.properties >/dev/null 2>&1; then
        echo "tls-selftest: the WRONG CA was accepted - verification is not happening" >&2
        exit 1
    fi
    echo "  tls-selftest: the right CA connects, the wrong CA does not"
    ;;
  ssl-offsets)
    # Deliberately absent: offsets are read over PLAINTEXT even when the records arrived over TLS,
    # so the path that verifies the claim is not the path the claim is about. Use `offsets`.
    echo "offsets are read over plaintext on purpose - use: broker.sh offsets <topic>" >&2
    exit 2
    ;;
  down)
    docker compose -f "$COMPOSE" -f "$TLS_COMPOSE" down < /dev/null >/dev/null 2>&1
    ;;
  topic)
    kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists --topic "$2" \
        --partitions "$PARTITIONS" --replication-factor 1 >/dev/null 2>&1
    kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$2" 2>/dev/null
    ;;
  strict-topic)
    # min.insync.replicas=2 on a single-broker cluster: a valid `acks=all` is then refused by the
    # BROKER with NOT_ENOUGH_REPLICAS, while acks=1 succeeds. That is what proves the setting
    # travels - an invalid value proves nothing, because the JVM client refuses it locally before
    # any broker sees it (B-06).
    kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists --topic "$2" --partitions 1 --replication-factor 1 \
        --config min.insync.replicas=2 >/dev/null 2>&1
    kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$2" 2>/dev/null
    ;;
  offsets)
    # The summed end offsets of every partition: the oracle for "how many records actually landed".
    kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$2" 2>/dev/null \
        | awk -F: '{ s += $3 } END { print s + 0 }'
    ;;
  consume)
    # -a because a payload may carry NUL bytes and some greps then treat the stream as binary.
    kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$2" --from-beginning --timeout-ms "${CONSUME_MS:-15000}" 2>/dev/null \
        | grep -ac "$3"
    ;;
  headers)
    # What the broker actually stored, read by a third party. `print.headers` is the only way to see
    # them at all from outside: nothing in this library reads back, and a producer checked by its own
    # consumer can be wrong in both directions at once.
    kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$2" --from-beginning --timeout-ms "${CONSUME_MS:-15000}" \
        --property print.headers=true --property print.value=true \
        --property headers.separator=, --property key.separator=$'\t' 2>/dev/null \
        | grep -a "$3" | head -1
    ;;
  keys)
    # Every key on the topic, one per line, read by the broker's own consumer. This is the oracle for
    # "which records actually landed": counting offsets would answer a weaker question, because a
    # count cannot say WHICH event is the one that is missing.
    kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$2" --from-beginning --timeout-ms "${CONSUME_MS:-15000}" \
        --formatter-property print.key=true --formatter-property print.value=false 2>/dev/null
    ;;
  produce)
    # Reads stdin. Note kci, not kc.
    kci /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$2" --producer-property acks=all 2>/dev/null
    ;;
  selftest)
    # Every helper above must be able to say NO, or a later green says nothing. Run against a port
    # with nothing on it: each one has to fail rather than quietly return zero.
    BOOTSTRAP=127.0.0.1:9099 kc /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server 127.0.0.1:9099 >/dev/null 2>&1
    test $? -ne 0 || { echo "selftest: the broker check passed against a dead port"; exit 1; }
    echo "selftest: the broker check fails against a dead port, as it must"
    ;;
  *)
    echo "usage: broker.sh up|tls-up|down|topic <name>|offsets <name>|keys <name>|consume <name> <pattern>|produce <name>|selftest|tls-selftest" >&2
    exit 2
    ;;
esac
