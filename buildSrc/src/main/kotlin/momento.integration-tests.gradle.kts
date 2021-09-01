/***********************************************************************************************************************
 * Re-usable gradle plugin to configure a java/kotlin module with an `integrationTest` task, separate from unit tests.
 **********************************************************************************************************************/

import org.gradle.api.tasks.testing.logging.TestExceptionFormat

// Set up integration testing -------

// https://docs.gradle.org/current/userguide/java_testing.html#sec:configuring_java_integration_tests

configure<JavaPluginExtension> {

    // Define independent source set so we can run separately then unit tests
    sourceSets {
        create("intTest") {
            compileClasspath += sourceSets.get("main").output + configurations.getByName("testRuntimeClasspath")
            runtimeClasspath += output + compileClasspath
        }
    }

    // Extend base project run time and test configuration then add integration specific deps
    val intTestImplementation: Configuration by configurations.getting {
        extendsFrom(configurations.getByName("testImplementation"))
    }
    configurations["intTestRuntimeOnly"].extendsFrom(configurations.getByName("runtimeOnly"))

    // Define separate task for running int tests
    val integrationTest = task<Test>("integrationTest") {
        description = "Runs the integration tests"
        group = "verification"
        testClassesDirs = sourceSets["intTest"].output.classesDirs
        classpath = sourceSets["intTest"].runtimeClasspath
        useJUnitPlatform()
        testLogging {
//         showStandardStreams = true  // Un comment this if need full integration test output for stdout & stderr
            exceptionFormat = TestExceptionFormat.FULL
            events("passed", "skipped", "failed")
        }
        outputs.upToDateWhen { false }
    }
}
