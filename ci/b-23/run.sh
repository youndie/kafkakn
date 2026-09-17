#!/usr/bin/env bash
# B-23: the same twenty rounds against a publisher whose publish returns BEFORE the acknowledgement.
#
# B-19 measured the shape where a record is either inside somebody's `send` or finished, so `close`
# never had anything to flush and half of the producer's contract about it was never exercised. This
# is the other shape: a bounded queue in front of the producer, which is what a service on an ingress
# path actually writes, because nobody wants a webhook's `200` to wait for Kafka.
#
# IT IS THE SAME HARNESS, with `QUEUE` set. Two scripts measuring two arms would drift, and the whole
# value here is that the two runs are comparable line for line.
#
# WHAT EACH LOSS MEANS IS FIXED BEFORE THE RUN, because deciding it afterwards decides the result:
#
#   producer=N  a record the process ASKED the producer to send - the log says so, at the time - and
#               which never arrived and was never reported as refused. This is a kafkakn question and
#               it is what the rounds are judged on.
#   outbox=N    a record accepted, queued, and never reached the producer at all. This is an OUTBOX
#               question - whether a service should record its intent and reconcile later - and it is
#               NOT about this library. It is counted, named and kept out of the verdict.
#   refused=N   a send that threw. The contract allows exactly this ("acknowledged, or its send
#               throws"), so it is a legitimate non-delivery in either arm.
#
#   ci/b-23/run.sh [rounds]
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
# 64 deep: the same order as the concurrency B-19's second sweep used, so that the queue is a real
# window rather than a formality, and small enough that a full one is backpressure on the ingress
# within a round rather than after it.
export QUEUE=${QUEUE:-64}
export TAG=${TAG:-rq-a-queued}
export WORK=${WORK:-/tmp/kafkakn-b-23}
export CONCURRENCY=${CONCURRENCY:-64}
exec bash "$HERE/../b-19/run.sh" "$@"
