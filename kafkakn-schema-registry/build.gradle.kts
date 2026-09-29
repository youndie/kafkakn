// Schema Registry support (stage 21): a client for the registry's REST API, and serializers that put a
// `@Serializable` type into the registry's wire format. A module of its own so that `kafkakn-core` gains no HTTP
// client (B-92). The transport is a Ktor `HttpClient`, CIO by default; a native caller who needs HTTPS passes Curl.
plugins {
    alias(wip.plugins.kotlinMultiplatform)
    // For the suite's `@Serializable` types; the module itself only reads descriptors.
    alias(wip.plugins.kotlinSerialization)
    id("io.github.youndie.sborka.kmp")
    id("io.github.youndie.sborka.lint")
    // B-99: published beside kafkakn-core, under the same numbered version.
    id("io.github.youndie.sborka.publish")
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
            // B-95: at the portfolio's serialization version, read from its catalogue so the number lives in one place.
            api("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:${wip.versions.serialization.get()}")
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(wip.kotlinx.coroutines.test)
        }
        // B-98: a record encoded through the registry over HTTPS and produced over TLS, in one process. On the JVM
        // only, by the owner's decision: on native, Ktor's Curl and kafkakn-core each carry a static OpenSSL, and a
        // binary with both does not link.
        jvmTest.dependencies {
            implementation(project(":kafkakn-core"))
        }
        // B-98: how the suite shows that Curl reaches an HTTPS registry from native, in a binary without
        // kafkakn-core. A test dependency only; the module does not depend on it.
        nativeTest.dependencies {
            implementation(libs.ktor.client.curl)
        }
    }
}

// Three coordinates, as for kafkakn-core: the metadata module, `-jvm` and `-linuxx64`. `ci/publish/run.sh` publishes
// here first and names each one before anything is uploaded.
publishing {
    repositories {
        maven {
            name = "local"
            url = uri(rootProject.layout.buildDirectory.dir("local-repo"))
        }
    }
}
