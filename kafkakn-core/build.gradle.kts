// The one published module. Nothing is implemented; this declares the shape.
//
// Both targets from the first commit, not linuxX64 with the JVM "later": the JVM arm is the oracle
// (research §1.1), and an oracle added after the implementation is an oracle shaped by it.

plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvmToolchain(21)

    jvm()
    linuxX64()
    // linuxArm64 is designed for and not declared (research D6). Adding it is a line here and a
    // build-matrix row; nothing in common code names a target, so it stays that way.

    sourceSets {
        commonTest.dependencies {
            implementation(kotlin("test"))
        }
    }
}
