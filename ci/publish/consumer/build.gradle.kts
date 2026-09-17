plugins {
    kotlin("multiplatform") version "2.4.20"
}

kotlin {
    jvmToolchain(21)
    jvm()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            // The coordinate a stranger would write. If the metadata module is missing, or carries
            // the wrong variants, this line is where it shows.
            implementation("io.github.youndie:kafkakn-core:${providers.gradleProperty("kafkakn.version").get()}")
        }
    }
}
