plugins {
    kotlin("multiplatform") version "2.4.20"
}

kotlin {
    jvmToolchain(21)

    jvm {
        binaries {
            executable { mainClass.set("MainKt") }
        }
    }
    linuxX64 {
        binaries.executable { entryPoint = "main" }
    }

    sourceSets {
        commonMain.dependencies {
            implementation("io.github.youndie.kafkakn:kafkakn-core:${providers.gradleProperty("kafkakn.version").get()}")
        }
    }
}
