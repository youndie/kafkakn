package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-58](../../../../../../../docs/backlog/B-58-list-and-describe-consumer-groups.md): list and describe
 * consumer groups through the admin client.
 *
 * A group with one live member reading the consumer fixture is listed and described. The same group with
 * no member left, and a group that does not exist, are described too. What each arm says about the last
 * two is recorded and compared (compare-arms.sh), and `ci/b-58/run.sh` holds the live description against
 * `kafka-consumer-groups.sh --describe --members`.
 */
class AdminGroupsTest {
    @Test
    fun a_group_is_listed_and_described_as_the_broker_describes_it() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val group = "kafkakn-admin-group-$armName-${randomSuffix()}"
                val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                val consumer =
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to group,
                            "client.id" to "kafkakn-described-$armName",
                            "auto.offset.reset" to "earliest",
                        ),
                    )
                try {
                    consumer.subscribe(listOf(consumeTopic))
                    val until = TimeSource.Monotonic.markNow() + ASSIGNED_WITHIN
                    while (consumer.assignment().isEmpty() && until.hasNotPassedNow()) consumer.poll(POLL)
                    consumer.commit()

                    val listed = admin.listConsumerGroups().singleOrNull { it.groupId == group }
                    assertEquals(GroupState.STABLE, listed?.state, "listed, with its state")

                    val described = admin.describeConsumerGroups(listOf(group)).getValue(group)
                    assertEquals(GroupState.STABLE, described.state)
                    val member = described.members.single()
                    assertEquals("kafkakn-described-$armName", member.clientId)
                    assertEquals(listOf(TopicPartition(consumeTopic, 0)), member.assignment)
                    assertTrue(member.memberId.isNotEmpty(), "a member id")
                    recordArmFact("admin.group", group)
                    recordArmFact("admin.group.member", member.memberId)
                    recordArmFact("admin.group.host", member.host)
                    recordObservation("admin.group.assignor", described.partitionAssignor)
                    recordObservation("admin.group.state", described.state.name)
                } finally {
                    consumer.close()
                }
                try {
                    // The same group with no member left: it has a commit, so it still exists.
                    val empty = admin.describeConsumerGroups(listOf(group)).getValue(group)
                    recordObservation("admin.group.empty", "${empty.state} members=${empty.members.size}")
                    assertEquals(
                        "EMPTY members=0",
                        "${empty.state} members=${empty.members.size}",
                        "a group with no member left",
                    )
                    // A group nobody ever joined: DEAD with no members on both arms (B-58). The Java client throws
                    // for it and librdkafka does not, and this is where the two were made one.
                    val missing =
                        outcome { admin.describeConsumerGroups(listOf("kafkakn-no-such-group-${randomSuffix()}")) }
                    assertEquals("DEAD members=0", missing, "a group that does not exist")
                    recordObservation("admin.group.missing", missing)
                } finally {
                    admin.close()
                }
            }
        }

    /**
     * A group whose member is not kafkakn: the distribution's console consumer, started by `ci/b-58/run.sh`,
     * which names the group through the environment and holds this description against
     * `kafka-consumer-groups.sh --describe --members --verbose`.
     */
    @Test
    fun a_group_of_a_third_party_is_described_as_the_broker_tool_describes_it() =
        runTest(timeout = 1.minutes) {
            val group = testEnv("KAFKAKN_DESCRIBE_GROUP")
            if (group == null) {
                recordArmFact("admin.third.group", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                try {
                    val described = admin.describeConsumerGroups(listOf(group)).getValue(group)
                    val members =
                        described.members.joinToString(";") { member ->
                            "${member.memberId},${member.host},${member.clientId}," +
                                member.assignment.joinToString("+") { "${it.topic}:${it.partition}" }
                        }
                    recordArmFact("admin.third.members", members)
                    recordObservation("admin.third.state", described.state.name)
                    recordObservation("admin.third.members", members)
                } finally {
                    admin.close()
                }
            }
        }

    /** What a call returned, rendered, or the exception it threw: the same text on both arms, or a finding. */
    private suspend fun outcome(call: suspend () -> Map<String, ConsumerGroupDescription>): String =
        try {
            call().values.joinToString { "${it.state} members=${it.members.size}" }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (thrown: Exception) {
            "threw ${thrown::class.simpleName}"
        }

    private companion object {
        val POLL = 200.milliseconds
        val ASSIGNED_WITHIN = 30.seconds
    }
}
