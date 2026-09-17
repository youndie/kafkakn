// A KOTLIN/NATIVE BINARY WITH NO KAFKA IN IT, and nothing else.
//
// It exists to be the other half of one `ldd`. The README claims the runtime dependencies of a
// binary that links kafkakn are the same as those of one that does not, and a claim like that is
// only worth making if both binaries are in front of you — built by the same compiler, for the same
// target, the same way.
rootProject.name = "kafkakn-ldd-baseline"

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
