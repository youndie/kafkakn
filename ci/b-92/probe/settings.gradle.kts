// B-92: can a Kotlin/Native binary speak HTTPS through Ktor's CIO client, and what does it cost? A build of its own,
// knowing nothing of kafkakn, so its answer is about the client alone.
rootProject.name = "kafkakn-http-probe"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
