package io.github.youndie.kafkakn.schema

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.encodeURLPathPart
import io.ktor.http.isSuccess
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/** The schema formats this module writes: the two that are real on both targets (B-92). Avro is not one of them. */
public enum class SchemaType {
    JSON,
    PROTOBUF,
}

/** A schema as the registry holds it: its [id], its [type] and its text. */
public data class RegisteredSchema(
    public val id: Int,
    public val type: SchemaType,
    public val schema: String,
)

/**
 * The registry refused or failed a request. [status] is the HTTP status; [errorCode] is the registry's own, when it
 * gave one: `42201` an invalid schema, `40403` a schema id it does not have, `409` an incompatible schema.
 */
public class SchemaRegistryException(
    public val status: Int,
    public val errorCode: Int?,
    message: String,
) : RuntimeException(message)

/** The subject a topic's record values are registered under: Confluent's default, `TopicNameStrategy`. */
public fun valueSubject(topic: String): String = "$topic-value"

/** The subject a topic's record keys are registered under, by the same strategy. */
public fun keySubject(topic: String): String = "$topic-key"

/**
 * A Schema Registry client ([B-93](../../../../../../../docs/backlog/B-93-a-registry-client.md)): registers a schema
 * under a subject and reads one back by id, over the registry's REST API.
 *
 * **Both answers are cached**, so a record costs no request once its schema is known: [register] remembers the id of
 * each (subject, type, schema) it has registered, and [schema] each schema it has read. The registry gives the same
 * id for the same schema, so neither can go stale.
 *
 * **The transport is a Ktor [HttpClient]**, CIO by default, which speaks HTTP on both targets and HTTPS on the JVM.
 * CIO has no TLS on Kotlin/Native (B-92): a native caller who needs HTTPS passes an `HttpClient(Curl)`, and takes
 * libcurl as a dependency of their binary by that choice. A client passed in is the caller's to close; the default
 * one is closed by [close].
 */
public class SchemaRegistry(
    url: String,
    httpClient: HttpClient? = null,
) {
    private val base = url.trimEnd('/')
    private val http = httpClient ?: HttpClient(CIO)
    private val owned = httpClient == null
    private val lock = Mutex()
    private val ids = mutableMapOf<Triple<String, SchemaType, String>, Int>()
    private val schemas = mutableMapOf<Int, RegisteredSchema>()

    /**
     * The id of [schema] under [subject], registering it if the subject does not hold it yet. Asks the registry
     * once per (subject, type, schema); later calls are answered from the cache.
     */
    public suspend fun register(
        subject: String,
        schema: String,
        type: SchemaType,
    ): Int {
        val key = Triple(subject, type, schema)
        lock.withLock { ids[key] }?.let { return it }
        val response =
            http.post("$base/subjects/${subject.encodeURLPathPart()}/versions") {
                header(HttpHeaders.ContentType, CONTENT_TYPE)
                header(HttpHeaders.Accept, CONTENT_TYPE)
                setBody(
                    buildJsonObject {
                        put("schema", schema)
                        put("schemaType", type.name)
                    }.toString(),
                )
            }
        val id = answer(response, "register under $subject").getValue("id").jsonPrimitive.int
        lock.withLock {
            ids[key] = id
            schemas.getOrPut(id) { RegisteredSchema(id, type, schema) }
        }
        return id
    }

    /** The schema the registry holds under [id], asked once and then answered from the cache. */
    public suspend fun schema(id: Int): RegisteredSchema {
        lock.withLock { schemas[id] }?.let { return it }
        val response = http.get("$base/schemas/ids/$id") { header(HttpHeaders.Accept, CONTENT_TYPE) }
        val body = answer(response, "schema $id")
        // The registry leaves `schemaType` out for Avro, its default. Avro is not a type this module reads.
        val typeName = body["schemaType"]?.jsonPrimitive?.content ?: "AVRO"
        val type =
            SchemaType.entries.firstOrNull { it.name == typeName }
                ?: throw SchemaRegistryException(
                    response.status.value,
                    null,
                    "schema $id is $typeName, which this module does not read",
                )
        val read = RegisteredSchema(id, type, body.getValue("schema").jsonPrimitive.content)
        lock.withLock { schemas[id] = read }
        return read
    }

    /** Closes the HTTP client this registry made for itself; one passed in is left to its owner. */
    public fun close() {
        if (owned) http.close()
    }

    private suspend fun answer(
        response: HttpResponse,
        what: String,
    ): JsonObject {
        val text = response.bodyAsText()
        val body = jsonOrNull(text)
        if (response.status.isSuccess() && body != null) return body
        val said = body?.get("message")?.jsonPrimitive?.content ?: text.take(MESSAGE)
        throw SchemaRegistryException(
            status = response.status.value,
            errorCode = body?.get("error_code")?.jsonPrimitive?.intOrNull,
            message = "$what: ${response.status.value} $said",
        )
    }

    /** The body as a JSON object, or null for one that is not: a proxy's HTML page, an empty 502. */
    private fun jsonOrNull(text: String): JsonObject? =
        try {
            Json.parseToJsonElement(text).jsonObject
        } catch (notJson: IllegalArgumentException) {
            null
        }

    private companion object {
        const val CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
        const val MESSAGE = 200
    }
}
