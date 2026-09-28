import io.github.youndie.kafkakn.schema.SchemaRegistry
import io.github.youndie.kafkakn.schema.jsonSchemaSerde
import io.github.youndie.kafkakn.schema.protobufSerde
import io.github.youndie.kafkakn.schema.valueSubject
import kotlinx.serialization.builtins.serializer

/**
 * Compiled, not run (B-99). The registry module's common surface, named the way a caller names it: a registry, a
 * subject, and both serdes over a serializer the caller holds. A metadata module without a usable common surface, or a
 * platform without its `actual`, fails here.
 */
@Suppress("unused")
suspend fun registryProbe(url: String): ByteArray {
    val registry = SchemaRegistry(url)
    try {
        registry.protobufSerde(valueSubject("probe"), Long.serializer())
        return registry.jsonSchemaSerde(valueSubject("probe"), String.serializer()).encode("hello")
    } finally {
        registry.close()
    }
}
