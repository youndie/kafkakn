plugins {
    alias(wip.plugins.kotlinMultiplatform) apply false
}

// group and version come from gradle.properties, which Gradle applies to every project. Repeating
// them per module is how a module ends up published under a coordinate nobody meant.
