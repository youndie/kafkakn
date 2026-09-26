// B-70: a service built on kafkakn, for the soak run in ci/b-70/run.sh. Not published: it is this project's
// own consumer, not a product. One service in common code, built as a native executable and as a JVM
// program, so one group holds members of both arms.

plugins {
    alias(wip.plugins.kotlinMultiplatform)
    id("io.github.youndie.sborka.kmp")
    id("io.github.youndie.sborka.lint")
}

kotlin {
    jvm()
    linuxX64 {
        binaries.executable {
            entryPoint = "io.github.youndie.kafkakn.soak.main"
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(project(":kafkakn-core"))
        }
    }
}

// The JVM instance's launch line, where ci/b-70/run.sh reads it: the toolchain's java and the runtime
// classpath. A JavaExec task would put Gradle between the runner and the process it has to kill and freeze.
val soakJvmLaunch by tasks.registering {
    val main = kotlin.jvm().compilations.getByName("main")
    val classpath = main.output.allOutputs + main.runtimeDependencyFiles
    val java = javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(21)) }
    val launch = layout.buildDirectory.file("soak/jvm.launch")
    dependsOn(main.compileTaskProvider)
    inputs.files(classpath)
    outputs.file(launch)
    doLast {
        launch.get().asFile.apply {
            parentFile.mkdirs()
            writeText(
                "JAVA=${java.get().executablePath.asFile.absolutePath}\nCP=${classpath.files.joinToString(":")}\n",
            )
        }
    }
}
