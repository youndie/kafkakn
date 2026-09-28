plugins {
    kotlin("multiplatform") version "2.4.20"
}

kotlin {
    jvmToolchain(21)
    jvm()
    linuxX64 {
        binaries.executable { entryPoint = "main" }
    }

    sourceSets {
        commonMain.dependencies {
            // The portfolio's Ktor, from sborka's catalogue.
            implementation("io.ktor:ktor-client-cio:3.5.2")
        }
    }
}
