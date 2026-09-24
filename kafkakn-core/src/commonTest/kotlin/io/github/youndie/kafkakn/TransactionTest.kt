package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

/**
 * [B-30](../../../../../../../docs/backlog/B-30-transactions.md): records that become visible together,
 * or not at all.
 *
 * **The oracle changes here, and that is the first thing this suite says.** Every accounting check
 * before this item compared what was handed in with the topic's end offsets. A transaction's commit
 * or abort marker occupies an offset too, so on a transactional topic the end offset stops being a
 * count. What `ci/b-30/run.sh` counts instead is RECORDS, read by `kafka-console-consumer` twice:
 * under `read_committed`, which is the claim, and under `read_uncommitted`, which is what shows an
 * aborted transaction's records were really sent — a zero under `read_committed` alone is also what
 * a producer that sent nothing produces.
 *
 * The transactional ids are recorded as well, and the broker's own `kafka-transactions.sh describe`
 * says how each transaction ended.
 */
class TransactionTest {
    @Test
    fun records_in_a_committed_transaction_are_all_visible_under_read_committed() =
        runTest(timeout = TIMEOUT) {
            val id = transactionalId("commit")
            val stamp = "txn-commit-$armName-${randomSuffix()}"
            withTransactionalProducer(id) { producer ->
                producer.initTransactions()
                producer.beginTransaction()
                sendAll(producer, stamp)
                producer.commitTransaction()
            }
            record("commit", id, stamp)
        }

    @Test
    fun records_in_an_aborted_transaction_are_none_of_them_under_read_committed() =
        runTest(timeout = TIMEOUT) {
            val id = transactionalId("abort")
            val stamp = "txn-abort-$armName-${randomSuffix()}"
            withTransactionalProducer(id) { producer ->
                producer.initTransactions()
                producer.beginTransaction()
                // Every record is ACKNOWLEDGED before the abort: `send` returns only after the broker
                // wrote it. So the records are on the log, and only the marker decides who sees them.
                sendAll(producer, stamp)
                producer.abortTransaction()
            }
            record("abort", id, stamp)
        }

    @Test
    fun inTransaction_commits_on_return_and_aborts_on_a_throw() =
        runTest(timeout = TIMEOUT) {
            val id = transactionalId("helper")
            val committed = "txn-helper-commit-$armName-${randomSuffix()}"
            val aborted = "txn-helper-abort-$armName-${randomSuffix()}"
            withTransactionalProducer(id) { producer ->
                producer.initTransactions()
                producer.inTransaction { sendAll(producer, committed) }
                assertFailsWith<ChangedMyMind> {
                    producer.inTransaction {
                        sendAll(producer, aborted)
                        throw ChangedMyMind()
                    }
                }
                // The same producer goes on: an aborted transaction is not a broken producer.
                producer.inTransaction { }
            }
            recordArmFact("txn.helper-commit.stamp", committed)
            recordArmFact("txn.helper-abort.stamp", aborted)
            recordArmFact("txn.count", RECORDS.toString())
        }

    @Test
    fun a_fenced_producer_fails_with_one_kafkakn_exception_on_both_arms() =
        runTest(timeout = TIMEOUT) {
            val id = transactionalId("fenced")
            val stamp = "txn-fenced-$armName-${randomSuffix()}"
            withTransactionalProducer(id) { first ->
                first.initTransactions()
                first.beginTransaction()
                sendAll(first, stamp)
                // A second producer with the same id: its init fences the first and aborts the first's
                // open transaction.
                withTransactionalProducer(id) { second -> second.initTransactions() }
                val fenced = assertFailsWith<ProducerFencedException> { first.commitTransaction() }
                // What each client actually said, underneath the one type - recorded, not compared.
                recordArmFact("txn.fenced.failure", fenced.chainText().replace('\n', ' ').take(REASON))
                // And it stays fenced: the documented "every later call fails" is a claim about the
                // next send too, which each client reaches by a different road.
                val later =
                    assertFailsWith<ProducerFencedException> {
                        withContext(Dispatchers.Default) {
                            first.send(ProducerRecord(testTopic, "$stamp:after".encodeToByteArray()))
                        }
                    }
                recordArmFact("txn.fenced.send.failure", later.chainText().replace('\n', ' ').take(REASON))
            }
            recordArmFact("txn.fenced.stamp", stamp)
        }

    private class ChangedMyMind : RuntimeException("the caller decided to abort")

    private fun transactionalId(what: String) = "kafkakn-txn-$what-$armName-${randomSuffix()}"

    private suspend fun sendAll(
        producer: KafkaProducer,
        stamp: String,
    ) = withContext(Dispatchers.Default) {
        repeat(RECORDS) { index -> producer.send(ProducerRecord(testTopic, "$stamp:$index".encodeToByteArray())) }
    }

    private fun record(
        what: String,
        id: String,
        stamp: String,
    ) {
        recordArmFact("txn.$what.id", id)
        recordArmFact("txn.$what.stamp", stamp)
        recordArmFact("txn.count", RECORDS.toString())
    }

    private suspend fun withTransactionalProducer(
        id: String,
        use: suspend (KafkaProducer) -> Unit,
    ) {
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "transactional.id" to id))
        try {
            use(producer)
        } finally {
            withContext(Dispatchers.Default) { producer.close() }
        }
    }

    private companion object {
        const val RECORDS = 50
        const val REASON = 400
        val TIMEOUT = 3.minutes
    }
}
