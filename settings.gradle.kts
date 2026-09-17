rootProject.name = "kafkakn"

include(":kafkakn-core")

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
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
