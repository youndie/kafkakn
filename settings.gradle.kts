rootProject.name = "kafkakn"

include(":kafkakn-core")

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        // Written out by hand, and it cannot be otherwise: `pluginManagement` is evaluated before
        // any settings plugin is applied - including the one that is fetched through it.
        maven("https://reposilite.kotlin.website/snapshots") {
            name = "wip-snapshots"
            // The filter is not politeness. An unfiltered repository is asked for EVERY coordinate
            // the build resolves, which costs a round trip per miss and lets an unrelated group be
            // answered by the wrong server.
            content { includeGroupByRegex("io\\.github\\.youndie.*") }
        }
    }
}

plugins {
    // Resolves a JDK for the toolchain rather than depending on whichever one happens to be on the
    // machine. The two arms must compile the same way on a laptop and on a build box.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"

    // The portfolio's settings half: the resolution repositories with their content filters, and
    // the shared `wip` catalogue beside this repository's own `libs`.
    //
    // The MODULE conventions - `sborka.kmp`, `sborka.lint`, `sborka.publish` - are deliberately not
    // taken here. They move the toolchain, the formatter and the whole publication block, and this
    // repository's publication is what B-12 and B-15 just finished measuring. That is its own
    // migration, with its own run of the acceptance.
    id("io.github.youndie.sborka.settings") version "0.4.0.86"
}

// `dependencyResolutionManagement` belongs to the settings plugin now: Maven Central and the
// snapshot repository - with the content filter this file used to spell out twice - are declared
// there, once, for every repository in the portfolio.
//
// "No Maven Central" was always a statement about where this project PUBLISHES (research D7), never
// about where it reads from. Kotlin, coroutines and kafka-clients live nowhere else.
