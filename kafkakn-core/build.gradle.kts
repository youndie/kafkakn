// The one published module. Nothing is implemented; this declares the shape.
//
// Both targets from the first commit, not linuxX64 with the JVM "later": the JVM arm is the oracle
// (research §1.1), and an oracle added after the implementation is an oracle shaped by it.

plugins {
    alias(wip.plugins.kotlinMultiplatform)
    // The portfolio's module half: the coordinate and the toolchain, explicit API and warnings as
    // errors, the formatter, and the publication with its POM and its `wip` repository. What stays
    // in this file is what is about THIS module - the targets, the C bundle, the cinterop seam.
    id("io.github.youndie.sborka.kmp")
    id("io.github.youndie.sborka.lint")
    id("io.github.youndie.sborka.publish")
}

// The C bundle, built by ci/librdkafka/build.sh into a cache outside the source tree. It is not
// built by Gradle: that would put a multi-minute Docker build on every clean checkout.
//
// `-Pkafkakn.noKafkaC` drops the cinterop, the linker options and the test that uses them, which is
// how a genuinely Kafka-free binary is produced to compare `ldd` against. It is a measurement aid,
// not a supported build.
val kafkaC: Boolean = findProperty("kafkakn.noKafkaC") == null
val bundle: String =
    (findProperty("kafkakn.bundle") as String?)
        ?: "${System.getProperty("user.home")}/.cache/kafkakn/librdkafka-${libs.versions.librdkafka.get()}"

kotlin {
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
            api(wip.kotlinx.coroutines.core)
        }
        jvmMain.dependencies {
            // The reference implementation. This arm delegates to it and adds as little as possible.
            implementation(libs.kafka.clients)
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            // runTest, so a suspending surface can be exercised from a common test on both arms.
            implementation(wip.kotlinx.coroutines.test)
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
    repositories {
        maven {
            name = "local"
            url = uri(rootProject.layout.buildDirectory.dir("local-repo"))
        }
    }
}
