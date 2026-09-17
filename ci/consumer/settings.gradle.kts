// A SERVICE THAT IS NOT PART OF THIS BUILD.
//
// It shares nothing with kafkakn: not the build, not the version catalogue, not a source set. Those
// are three of the things publication breaks, and a sample module inside the repository would have
// all three and could not notice any of them.
//
// What it knows is what a stranger knows: a coordinate and a repository URL.

rootProject.name = "kafkakn-consumer"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        // NO mavenLocal, and no project dependency. `~/.m2` is shared with every build on this
        // machine; resolving from it would prove the machine rather than the publication.
        // The URL is a parameter so the same consumer can be aimed at a CANDIDATE publication
        // before it goes out - a repository on disk holds the same bytes the server will. Defaults
        // to the real one, which is what a stranger would use.
        maven(providers.gradleProperty("kafkakn.repo").getOrElse("https://reposilite.kotlin.website/snapshots")) {
            content { includeGroupAndSubgroups("io.github.youndie") }
        }
        mavenCentral()
    }
}
