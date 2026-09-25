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

    // B-39: linuxArm64, on request only, as in kafkakn itself. Only the target is declared here. There are
    // no linker options and no paths, so the C must arrive inside the published klib, exactly as for
    // linuxX64.
    if (providers.gradleProperty("kafkakn.linuxArm64").isPresent) {
        linuxArm64 {
            binaries.executable { entryPoint = "main" }
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation("io.github.youndie.kafkakn:kafkakn-core:${providers.gradleProperty("kafkakn.version").get()}")
        }
    }
}
