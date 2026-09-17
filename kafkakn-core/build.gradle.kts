// The one published module. Nothing is implemented; this declares the shape.
//
// Both targets from the first commit, not linuxX64 with the JVM "later": the JVM arm is the oracle
// (research §1.1), and an oracle added after the implementation is an oracle shaped by it.

plugins {
    alias(libs.plugins.kotlin.multiplatform)
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
            }
            binaries.withType<org.jetbrains.kotlin.gradle.plugin.mpp.TestExecutable>().configureEach {
                // Absolute paths to the archives, not -l. `-lssl` would take a shared libssl
                // wherever one is installed and the binary would quietly stop being self-contained,
                // with nothing failing to say so.
                //
                // No -Xoverride-konan-properties and no extra -L: the C side was built against
                // glibc 2.17, so the toolchain's own sysroot is enough. That is the claim B-03
                // exists to check (research §1.3, D4).
                linkerOpts(
                    "$bundle/lib/librdkafka-static.a",
                    "$bundle/lib64/libssl.a",
                    "$bundle/lib64/libcrypto.a",
                    "$bundle/lib/libz.a",
                    "$bundle/lib/libzstd.a",
                    "-lpthread",
                    "-ldl",
                    "-lm",
                )
            }
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

