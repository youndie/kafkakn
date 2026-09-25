@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.StableRef
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.asStableRef
import kotlinx.cinterop.convert
import kotlinx.cinterop.cstr
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.set
import kotlinx.cinterop.staticCFunction
import kotlinx.cinterop.toKString
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import rdkafka.RD_KAFKA_RESP_ERR_NO_ERROR
import rdkafka.rd_kafka_oauthbearer_set_token
import rdkafka.rd_kafka_oauthbearer_set_token_failure
import rdkafka.rd_kafka_t

/**
 * The native half of the OAUTHBEARER bridge (B-33). librdkafka asks for a token through a refresh
 * callback it runs inside `rd_kafka_poll` — inside the producer's pump — so the callback cannot suspend
 * for the caller's provider. It starts a coroutine instead, which asks the provider and hands the answer
 * back with `rd_kafka_oauthbearer_set_token`, or `rd_kafka_oauthbearer_set_token_failure` carrying the
 * provider's own words. librdkafka calls it again at about eighty per cent of each token's life.
 *
 * Reached from the `staticCFunction` through the handle's opaque, a [StableRef] the producer disposes.
 */
internal class OAuthBearerBridge(
    private val provider: OAuthBearerTokenProvider,
    private val scope: CoroutineScope,
) {
    fun refresh(handle: CPointer<rd_kafka_t>) {
        scope.launch {
            val token =
                try {
                    provider.token()
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (failed: Exception) {
                    rd_kafka_oauthbearer_set_token_failure(
                        handle,
                        "OAUTHBEARER token provider failed: ${failed.message}",
                    )
                    return@launch
                }
            memScoped {
                val pairs = token.extensions.flatMap { (key, value) -> listOf(key, value) }
                val extensions = allocArray<CPointerVar<ByteVar>>(pairs.size.coerceAtLeast(1))
                pairs.forEachIndexed { index, text -> extensions[index] = text.cstr.ptr }
                val errstr = allocArray<ByteVar>(ERRSTR)
                val err =
                    rd_kafka_oauthbearer_set_token(
                        handle,
                        token.value,
                        token.expiresAtMillis,
                        token.principal,
                        if (pairs.isEmpty()) null else extensions,
                        pairs.size.convert(),
                        errstr,
                        ERRSTR.convert(),
                    )
                if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    // A token librdkafka will not take - an empty principal, an expiry in the past - is
                    // a failure of this token, said in librdkafka's words.
                    rd_kafka_oauthbearer_set_token_failure(handle, "OAUTHBEARER token refused: ${errstr.toKString()}")
                }
            }
        }
    }

    private companion object {
        const val ERRSTR = 512
    }
}

/** librdkafka's refresh callback: find the bridge through the handle's context, and let it fetch. */
internal val oauthBearerRefresh =
    staticCFunction<CPointer<rd_kafka_t>?, CPointer<ByteVar>?, COpaquePointer?, Unit> { handle, _, opaque ->
        val bridge = opaque?.asStableRef<HandleContext>()?.get()?.oauth ?: return@staticCFunction
        bridge.refresh(handle ?: return@staticCFunction)
    }
