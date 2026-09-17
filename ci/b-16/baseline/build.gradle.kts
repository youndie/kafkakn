plugins {
    // The same version `ci/consumer` pins. A baseline built by a different compiler would measure
    // the compiler as well as the library, and the difference is the only thing being asked about.
    kotlin("multiplatform") version "2.4.20"
}

kotlin {
    linuxX64 {
        binaries.executable { entryPoint = "main" }
    }

    sourceSets {
        // COROUTINES ARE HERE ON PURPOSE, and the first run of this comparison is why. A bare
        // hello-world differs from `ci/consumer` in two things — kafkakn and kotlinx-coroutines —
        // and a difference with two possible causes attributes nothing. The baseline is the consumer
        // with the library taken out, not the smallest program that compiles.
        linuxX64Main.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
        }
    }
}
