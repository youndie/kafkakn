plugins {
    kotlin("multiplatform") version "2.4.20"
}

kotlin {
    jvmToolchain(21)

    jvm {
        binaries {
            // A real executable, not a test. The point of this project is to be run.
            executable { mainClass.set("MainKt") }
        }
    }
    linuxX64 {
        // LINKED, not merely compiled. A klib that compiles against a cinterop and cannot be linked
        // into a binary is a library nobody can ship, and compiling is where a downstream build stops
        // noticing.
        binaries.executable { entryPoint = "main" }
    }

    sourceSets {
        commonMain.dependencies {
            implementation("io.github.youndie.kafkakn:kafkakn-core:${providers.gradleProperty("kafkakn.version").get()}")
        }
    }
}
