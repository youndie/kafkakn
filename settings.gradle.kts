rootProject.name = "kafkakn"

include(":kafkakn-core")

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()

        // Where this project PUBLISHES, declared here as well because the consumer acceptance in
        // B-13 resolves from it. The content filter is not politeness: an unfiltered repository is
        // asked about every dependency in the build, so an outage at this host would fail the
        // resolution of Kotlin and coroutines too - measured in a sibling project, where an
        // unreachable third-party repository broke the resolution of OUR artefact.
        maven("https://reposilite.kotlin.website/snapshots") {
            content { includeGroupAndSubgroups("io.github.youndie") }
        }
    }
}

plugins {
    // Resolves a JDK for the toolchain rather than depending on whichever one happens to be on the
    // machine. The two arms must compile the same way on a laptop and on a build box.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

dependencyResolutionManagement {
    repositories {
        // Maven Central is where this project READS from - Kotlin, coroutines, kafka-clients live
        // nowhere else. "No Maven Central" is a statement about where this project PUBLISHES, which
        // is reposilite and nothing else (research D7); the two are easy to confuse and this comment
        // exists so nobody resolves the confusion by breaking the build.
        mavenCentral()
    }
}
