// B-94: the registry's own serializers, run as a program of their own. kafkakn is nowhere on its classpath, so bytes
// it reads are checked by the registry's code, not by kafkakn's.
rootProject.name = "kafkakn-registry-oracle"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // Confluent's serializers are published here, under Apache 2.0 (B-92), and not on Maven Central.
        maven("https://packages.confluent.io/maven/")
    }
}
