// The one published module. Nothing is implemented; this declares the shape.
//
// Both targets from the first commit, not linuxX64 with the JVM "later": the JVM arm is the oracle
// (research §1.1), and an oracle added after the implementation is an oracle shaped by it.

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    `maven-publish`
}

// The C bundle, built by ci/librdkafka/build.sh into a cache outside the source tree. It is not
// built by Gradle: that would put a multi-minute Docker build on every clean checkout.
//
// `-Pkafkakn.noKafkaC` drops the cinterop, the linker options and the test that uses them, which is
// how a genuinely Kafka-free binary is produced to compare `ldd` against. It is a measurement aid,
// not a supported build.
val kafkaC: Boolean = findProperty("kafkakn.noKafkaC") == null
val bundle: String = (findProperty("kafkakn.bundle") as String?)
    ?: "${System.getProperty("user.home")}/.cache/kafkakn/librdkafka-${libs.versions.librdkafka.get()}"

kotlin {
    jvmToolchain(21)

    jvm()
    linuxX64 {
        if (kafkaC) {
            compilations.getByName("main").cinterops.create("rdkafka") {
                definitionFile.set(file("src/nativeInterop/cinterop/rdkafka.def"))
                includeDirs("$bundle/include")
                // Where the archives named in the .def are found. cinterop copies them INTO the
                // klib, so a consumer links against the published artefact and nothing else - which
                // is what B-13 found this project could not do.
                extraOpts("-libraryPath", "$bundle/lib", "-libraryPath", "$bundle/lib64")
            }
            // NO linkerOpts, and their absence is the check. The archives now travel inside the
            // cinterop klib (`staticLibraries` in rdkafka.def), so this project's own test binaries
            // link exactly the way a stranger's binary does. While they were named here, the suite
            // linked and the published artefact did not - and nothing in the gate could tell.
        }
    }
    // linuxArm64 is designed for and not declared (research D6). Adding it is a line here and a
    // build-matrix row; nothing in common code names a target, so it stays that way.

    sourceSets {
        if (kafkaC) {
            // Only present when the C bundle is: with -Pkafkakn.noKafkaC this directory is not a
            // source root, so the Kafka-free binary genuinely contains none of it.
            named("linuxX64Test") { kotlin.srcDir("src/linuxX64TestCinterop/kotlin") }
        }
        commonMain.dependencies {
            // api, not implementation: the public surface is suspend functions, so a consumer needs
            // coroutines on its own compile classpath to call them at all.
            api(libs.coroutines.core)
        }
        jvmMain.dependencies {
            // The reference implementation. This arm delegates to it and adds as little as possible.
            implementation(libs.kafka.clients)
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            // runTest, so a suspending surface can be exercised from a common test on both arms.
            implementation(libs.coroutines.test)
        }
    }
}

// A KMP module has as many coordinates as it has targets, and a publication route that covers one
// does not cover the others. The Kotlin plugin creates all three publications itself:
//
//   io.github.youndie:kafkakn-core           the metadata module - what a common consumer asks for
//   io.github.youndie:kafkakn-core-jvm       the jvm variant
//   io.github.youndie:kafkakn-core-linuxx64  the native variant
//
// `ci/publish/run.sh` asserts all three are present after a publish, because "it published" is a
// statement about a task, not about what a consumer can resolve.
publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
            name.set("kafkakn")
            description.set("A Kafka producer for Kotlin Multiplatform: librdkafka on native, the official client on the JVM")
            url.set("https://github.com/youndie/kafkakn")
            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            scm { url.set("https://github.com/youndie/kafkakn") }
        }
    }
    repositories {
        // A real Maven repository on disk. It is what the publication proof resolves from, and it
        // is deliberately NOT mavenLocal: `~/.m2` is shared with everything else on the machine, so
        // a consumer resolving from it can succeed on an artefact that was never published here.
        maven {
            name = "local"
            url = uri(rootProject.layout.buildDirectory.dir("local-repo"))
        }
        maven {
            name = "wip"
            url = uri("https://reposilite.kotlin.website/snapshots")
            credentials {
                // Gradle properties, which ORG_GRADLE_PROJECT_* supplies in CI. `orNull` rather
                // than `get()`: a developer without the credentials must still be able to configure
                // the build and publish to `local`.
                username = providers.gradleProperty("REPOSILITE_USER").orNull
                password = providers.gradleProperty("REPOSILITE_SECRET").orNull
            }
        }
    }
}
