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
MTLS_BOOTSTRAP=${MTLS_BOOTSTRAP:-127.0.0.1:9095}
SASL_BOOTSTRAP=${SASL_BOOTSTRAP:-127.0.0.1:9096}
SASL_SSL_BOOTSTRAP=${SASL_SSL_BOOTSTRAP:-127.0.0.1:9097}
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
    # The fixture's own topics, the ones the suite assumes rather than the ones a script names for a
    # run. Here once rather than in every script that runs the suite: there are six of those.
    kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic kafkakn-logappend --partitions "$PARTITIONS" --replication-factor 1 \
        --config message.timestamp.type=LogAppendTime >/dev/null 2>&1
    # B-29's topic: seven partitions, a count nothing else here has, so a description of the wrong
    # topic cannot pass for this one. Created here because the suite's default names it - §2.18 is
    # what a default nobody creates costs, and B-29 paid it again in its own full-suite run.
    kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic kafkakn-metadata --partitions 7 --replication-factor 1 >/dev/null 2>&1
    # B-36's topic: one partition holding twenty records written by the Kafka distribution's own client
    # (ci/harness/Records.java) - null keys, a tombstone, bytes that are not UTF-8, duplicate header
    # names. Written once: only when the partition is empty, so every run reads the same twenty.
    #
    # RETENTION OFF, and that is not tidiness. The records carry timestamps from 2023 on purpose - a
    # seek to a time needs one right answer - and time-based retention reads the RECORDS' time: with
    # the broker's default seven days the whole partition was deleted within minutes of being written,
    # and the next run found earliest = latest = 20. A topic already emptied that way is recreated.
    earliest=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic kafkakn-consume \
        --time -2 2>/dev/null | cut -d: -f3)
    if [ -n "$earliest" ] && [ "$earliest" != "0" ]; then
        kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --delete --topic kafkakn-consume >/dev/null 2>&1
        for _ in $(seq 1 15); do
            kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null \
                | grep -qx kafkakn-consume || break
            sleep 1
        done
    fi
    kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic kafkakn-consume --partitions 1 --replication-factor 1 --config retention.ms=-1 >/dev/null 2>&1
    if [ "$(bash "$0" offsets kafkakn-consume)" = "0" ]; then
        bash "$0" records write kafkakn-consume >/dev/null || { echo "  could not write kafkakn-consume" >&2; exit 1; }
    fi
    # SCRAM credentials (B-32), on every `up`: they live in the metadata log, which a recreated
    # container does not have. `--alter` replaces, so running it on a broker that has them is a no-op
    # in effect. PLAIN's users are in the JAAS file and need nothing here.
    #
    # One mechanism per call: both in one `--add-config` is refused by the broker - measured,
    # "A user credential cannot be altered twice in the same request".
    for mechanism in SCRAM-SHA-256 SCRAM-SHA-512; do
        kc /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter --entity-type users \
            --entity-name alice --add-config "$mechanism=[password=alice-secret]" >/dev/null 2>&1 \
            || { echo "  could not create the $mechanism credential" >&2; exit 1; }
    done
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
  sasl-selftest)
    # B-32's listeners, asked with the broker's own tools: the wrong password is refused for PLAIN and
    # for SCRAM, and only then are the right ones asked to connect - including SCRAM-SHA-512 over TLS,
    # which is the listener hosted Kafka looks like. A listener that accepted any password would make
    # every "connects" scenario green.
    api() { kc /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$1" \
        --command-config "/etc/kafka/secrets/$2" >/dev/null 2>&1; }
    for wrong in client-sasl-plain-wrong.properties client-sasl-scram256-wrong.properties; do
        if api "$SASL_BOOTSTRAP" "$wrong"; then
            echo "sasl-selftest: $wrong was accepted - the listener does not check passwords" >&2
            exit 1
        fi
    done
    for right in client-sasl-plain.properties client-sasl-scram256.properties; do
        api "$SASL_BOOTSTRAP" "$right" \
            || { echo "sasl-selftest: $right was refused - the fixture is broken" >&2; exit 1; }
    done
    api "$SASL_SSL_BOOTSTRAP" client-sasl-ssl-scram512.properties \
        || { echo "sasl-selftest: SCRAM-SHA-512 over TLS was refused - the fixture is broken" >&2; exit 1; }
    api "$SASL_BOOTSTRAP" client-sasl-oauthbearer.properties \
        || { echo "sasl-selftest: OAUTHBEARER with an unsigned token was refused - the fixture is broken" >&2; exit 1; }
    echo "  sasl-selftest: wrong passwords refused (PLAIN, SCRAM-SHA-256); PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 over TLS and OAUTHBEARER connect"
    ;;
  mtls-selftest)
    # B-31's listener, asked the same question first: can it say NO? A listener that was configured
    # to require a client certificate and quietly does not is exactly as green as one that works,
    # for every client that presents a good certificate - so the refusals are asked before the
    # acceptance, and the acceptance is asked at all so that "refused" cannot mean "down".
    api() { kc /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$MTLS_BOOTSTRAP" \
        --command-config "/etc/kafka/secrets/$1" >/dev/null 2>&1; }
    if api client-ssl.properties; then
        echo "mtls-selftest: a client with NO certificate was accepted - the listener asks nothing" >&2
        exit 1
    fi
    if api client-mtls-wrong.properties; then
        echo "mtls-selftest: a certificate from the WRONG authority was accepted" >&2
        exit 1
    fi
    api client-mtls.properties \
        || { echo "mtls-selftest: the RIGHT client certificate was refused - the fixture is broken" >&2; exit 1; }
    echo "  mtls-selftest: no certificate refused, the wrong authority's refused, the right one connects"
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
  values)
    # Every value on the topic, one per line, read by the broker's own consumer: the oracle for "was
    # anything written twice". Only meaningful for values that are unique by construction.
    #
    # ISOLATION is named every time rather than left to the tool's default (read_uncommitted), because
    # since B-30 the two answers differ on a transactional topic and a count that does not say which
    # one it is is not a count of anything. `--command-property`, not `--consumer-property`: the old
    # spelling still works in 4.3.1 but prints its deprecation notice on STDOUT, as a line of the
    # output - measured, it made a 200-record topic count 201.
    kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$2" --from-beginning --timeout-ms "${CONSUME_MS:-15000}" \
        --command-property "isolation.level=${ISOLATION:-read_uncommitted}" \
        --formatter-property print.value=true 2>/dev/null
    ;;
  records)
    # The consumer's third party (B-36): ci/harness/Records.java on the Kafka distribution's own client,
    # run on this host against the published port. The jars are the ones Gradle already resolved for
    # the JVM arm, pinned to the same version - kafkakn itself is not on the classpath.
    CLIENTS=$(find "$HOME/.gradle/caches/modules-2" -name 'kafka-clients-4.3.1.jar' | head -1)
    SLF4J=$(find "$HOME/.gradle/caches/modules-2" -name 'slf4j-api-*.jar' | sort | tail -1)
    [ -n "$CLIENTS" ] && [ -n "$SLF4J" ] || { echo "records: kafka-clients or slf4j-api is not in the Gradle cache" >&2; exit 1; }
    mode=$2
    shift 2
    java -cp "$CLIENTS:$SLF4J" "$HERE/Records.java" "$mode" "$BOOTSTRAP" "$@" 2>/dev/null
    ;;
  txn-state)
    # How the broker says a transactional id's last transaction ended - CompleteCommit, CompleteAbort
    # or anything else - from the transaction coordinator itself, not from the client that ran it.
    kc /opt/kafka/bin/kafka-transactions.sh --bootstrap-server "$BOOTSTRAP" describe \
        --transactional-id "$2" 2>/dev/null \
        | awk 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "TransactionState") c = i } NR == 2 { print $c }'
    ;;
  timed-values)
    # Every record's stored timestamp WITH ITS TYPE, then its value: "CreateTime:<ms>" or
    # "LogAppendTime:<ms>", a tab, the value. The type is the half the Java client cannot report.
    kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$2" --from-beginning --timeout-ms "${CONSUME_MS:-15000}" \
        --formatter-property print.timestamp=true --formatter-property print.value=true 2>/dev/null
    ;;
  partition-values)
    # One partition's values, from the beginning, read by the broker's own consumer: the oracle for
    # "is this record where it says it is" - a client's report of its own partition is not.
    kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$2" --partition "$3" --offset earliest --timeout-ms "${CONSUME_MS:-15000}" \
        --formatter-property print.value=true 2>/dev/null
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
    echo "usage: broker.sh up|tls-up|down|topic <name>|offsets <name>|keys <name>|consume <name> <pattern>|produce <name>|selftest|tls-selftest|mtls-selftest|sasl-selftest" >&2
    exit 2
    ;;
esac
