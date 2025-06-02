import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    id("momento.publishable-java-lib")
    id("momento.junit-tests")
    id("momento.integration-tests")
    id("com.diffplug.spotless") version "5.15.1"
}

dependencies {
    implementation(libs.momento.java.protos)

    api(libs.jsr305) // Marked api because the annotations are used in sdk methods
    api(libs.grpc.api) // Marked api because SdkException contains classes from this dependency
    implementation(libs.grpc.stub)
    implementation(libs.grpc.nettyshaded)
    implementation(libs.grpc.context)
    implementation(libs.protobuf.java)
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.slf4j.api)

    // For Auth token
    implementation(libs.jjwt.api)
    runtimeOnly(libs.jjwt.impl)
    runtimeOnly(libs.jjwt.gson)

    // Test dependencies
    testImplementation(libs.junit)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito)
    testImplementation(libs.mockito.jupiter)
    testImplementation(libs.slf4j.api)
    testImplementation(libs.logback)
}

spotless {
    java {
        removeUnusedImports()
        googleJavaFormat("1.11.0")
    }
}

// Modifying the JAR manifest so that can access release version in code
tasks.withType<Jar> {
    manifest {
        attributes["Implementation-Version"] = findProperty("version") as String
    }
}

// Disable dependency checking for tests because the plugin does not play well with our unusual test structure
tasks.named("analyzeIntTestClassesDependencies").configure {
    enabled = false
}
tasks.named("analyzeTestClassesDependencies").configure {
    enabled = false
}
tasks.named<Test>("integrationTest") {
    filter {
        excludeTestsMatching("momento.sdk.retry.*")
        excludeTestsMatching("momento.sdk.subscriptionInitialization.*")
    }
}

fun registerIntegrationTestTask(name: String, testClasses: List<String>) {
    tasks.register<Test>(name) {
        description = "Runs $name integration tests"
        group = "verification"

        val integrationTestTask = tasks.named<Test>("integrationTest").get()
        classpath = integrationTestTask.classpath
        testClassesDirs = integrationTestTask.testClassesDirs

        filter {
            testClasses.forEach { testClass ->
                includeTestsMatching(testClass)
            }
        }

        useJUnitPlatform()
        testLogging {
            exceptionFormat = TestExceptionFormat.FULL
            events("passed", "skipped", "failed")
        }
        outputs.upToDateWhen { false }
    }
}

registerIntegrationTestTask(
    "test-auth-service",
    listOf("momento.sdk.auth.*")
)

registerIntegrationTestTask(
    "test-cache-service",
    listOf("momento.sdk.cache.*")
)

registerIntegrationTestTask(
    "test-topics-service",
    listOf("momento.sdk.topics.*")
)

registerIntegrationTestTask(
    "test-leaderboard-service",
    listOf("momento.sdk.leaderboard.*")
)

registerIntegrationTestTask(
    "test-retries",
    listOf("momento.sdk.retry.*")
)
