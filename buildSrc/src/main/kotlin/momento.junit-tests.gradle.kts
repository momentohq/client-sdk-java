/***********************************************************************************************************************
 * Re-usable gradle plugin to configure a java/kotlin module with Jupiter Junit testing for unit tests
 **********************************************************************************************************************/

import org.gradle.api.tasks.testing.logging.TestExceptionFormat

tasks.withType<Test> {
    useJUnitPlatform()

    testLogging {
        // showStandardStreams = true  // Un comment this if need full integration test output for stdout & stderr
        exceptionFormat = TestExceptionFormat.FULL
        events("passed", "skipped", "failed")
    }
}
