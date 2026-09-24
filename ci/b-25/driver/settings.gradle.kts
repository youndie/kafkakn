// A PRODUCER THAT IS NOT PART OF THIS BUILD, driven through a broker that is frozen under it.
//
// It lives outside the suite on purpose: the fault is a `docker pause` issued by ci/b-25/run.sh while
// this process is producing, and a suite test cannot freeze the broker it is talking to. It resolves
// kafkakn the way a stranger does - a coordinate and a URL - pointed by the script at a candidate
// published to a repository on disk, so the code under test is this branch's.
rootProject.name = "kafkakn-idempotence-driver"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        maven(providers.gradleProperty("kafkakn.repo").get()) {
            content { includeGroupAndSubgroups("io.github.youndie") }
        }
        mavenCentral()
    }
}
