plugins {
    alias(wip.plugins.kotlinMultiplatform) apply false
    // Declared here, applied per module. The coordinate, the toolchain, the jvm floor, the style
    // and the publication come from these.
    alias(libs.plugins.sborkaKmp) apply false
    alias(libs.plugins.sborkaLint) apply false
    alias(libs.plugins.sborkaPublish) apply false
}

// group and version come from gradle.properties, which Gradle applies to every project. Repeating
// them per module is how a module ends up published under a coordinate nobody meant.
