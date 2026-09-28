// Schema Registry support (stage 21): a client for the registry's REST API, and serializers that put a
// `@Serializable` type into the registry's wire format. A module of its own so that `kafkakn-core` gains no HTTP
// client (B-92). The transport is a Ktor `HttpClient`, CIO by default; a native caller who needs HTTPS passes Curl.
plugins {
    alias(wip.plugins.kotlinMultiplatform)
    id("io.github.youndie.sborka.kmp")
    id("io.github.youndie.sborka.lint")
}

kotlin {
    jvm()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            api(libs.ktor.client.core)
            implementation(wip.kotlinx.coroutines.core)
            implementation(libs.ktor.client.cio)
            implementation(wip.kotlinx.serialization.json)
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(wip.kotlinx.coroutines.test)
        }
    }
}
