// A SEPARATE BUILD, not a module of kafkakn.
//
// That is the whole point: a module inside the same build resolves its neighbour through the
// project graph and never touches a repository, so it proves nothing about what was published. This
// one knows only a coordinate and a repository URL - the same two things a stranger has.

rootProject.name = "kafkakn-publication-probe"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    // NO mavenLocal. `~/.m2` is shared with every other build on the machine, and a probe that can
    // read it may succeed on an artefact this publication never produced.
    repositories {
        maven {
            name = "underTest"
            url = uri(providers.gradleProperty("kafkakn.repo").get())
        }
        mavenCentral()
    }
}
