@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.asStableRef
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.staticCFunction
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import platform.posix.size_t
import rdkafka.rd_kafka_t
import kotlin.concurrent.AtomicReference

/**
 * What librdkafka's callbacks reach through the handle's opaque — one per handle, because a handle has
 * one opaque: the OAUTHBEARER bridge when there is a provider (B-33), and the latest statistics
 * document (B-41).
 */
internal class HandleContext(
    val oauth: OAuthBearerBridge?,
) {
    val statistics = AtomicReference<String?>(null)
}

/**
 * librdkafka's statistics callback: keep the latest document, parse it only when asked. Returning 0
 * tells librdkafka to free the JSON itself.
 */
internal val statisticsReport =
    staticCFunction<CPointer<rd_kafka_t>?, CPointer<ByteVar>?, size_t, COpaquePointer?, Int> {
        _,
        json,
        length,
        opaque,
        ->
        val context = opaque?.asStableRef<HandleContext>()?.get()
        if (context != null && json != null) context.statistics.value = json.readBytes(length.toInt()).decodeToString()
        0
    }

/**
 * The portable metrics, read from one statistics document (librdkafka's STATISTICS.md): `msg_size`
 * for the buffered bytes; per broker entry, `waitresp_cnt` for requests in flight, `rtt.avg`
 * (microseconds, over the last statistics interval only) for the round trip, and `state == "UP"` for an
 * open connection. Every entry counts, the bootstrap one included, because the Java client's
 * `connection-count` counts its bootstrap socket too - measured: counting the cluster's brokers alone
 * gave 1 here against the Java client's 2 for the same single broker.
 */
internal fun metricsFrom(document: String?): ProducerMetrics {
    if (document == null) return ProducerMetrics(null, null, null, null)
    val root = Json.parseToJsonElement(document).jsonObject
    val brokers = (root["brokers"] as? JsonObject)?.values?.map { it.jsonObject }.orEmpty()
    val rtts =
        brokers.mapNotNull { broker ->
            val rtt = broker["rtt"] as? JsonObject ?: return@mapNotNull null
            val count = rtt["cnt"]?.jsonPrimitive?.longOrNull ?: 0
            rtt["avg"]?.jsonPrimitive?.doubleOrNull?.takeIf { count > 0 }
        }
    return ProducerMetrics(
        bufferedBytes = root["msg_size"]?.jsonPrimitive?.longOrNull,
        requestsInFlight = brokers.sumOf { it["waitresp_cnt"]?.jsonPrimitive?.intOrNull ?: 0 },
        brokerRoundTripMillis = if (rtts.isEmpty()) null else rtts.average() / MICROS_PER_MILLI,
        openConnections = brokers.count { it["state"]?.jsonPrimitive?.content == "UP" },
    )
}

private const val MICROS_PER_MILLI = 1000.0
