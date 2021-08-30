import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    `java-library`
    `maven-publish`
    idea
    java
}

group = "org.momento"
version = findProperty("version") as String

val opentelemetryVersion = rootProject.ext["opentelemetryVersion"]
var awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID") ?: findProperty("aws_access_key_id") as String? ?: ""
var awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY") ?: findProperty("aws_secret_access_key") as String? ?: ""

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven {
        name = "client-sdk-java"
        url = uri("s3://artifact-814370081888-us-west-2/client-sdk-java/release")
        credentials(AwsCredentials::class) {
            accessKey = awsAccessKeyId
            secretKey = awsSecretAccessKey
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("myLibrary") {
            from(components["java"])
        }
    }

    repositories {
        maven {
            url = uri("s3://artifact-814370081888-us-west-2/client-sdk-java/release")
            credentials(AwsCredentials::class) {
                accessKey = awsAccessKeyId
                secretKey = awsSecretAccessKey
            }
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")

    platform("io.opentelemetry:opentelemetry-bom:${opentelemetryVersion}")
    implementation("io.opentelemetry:opentelemetry-api:${opentelemetryVersion}")
    implementation("io.opentelemetry:opentelemetry-sdk:${opentelemetryVersion}")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:${opentelemetryVersion}")

    // Internal Deps -------------------
    implementation("io.grpc:grpc-netty:${rootProject.ext["grpcVersion"]}")
    implementation(project(":messages"))
}

tasks.test {
    useJUnitPlatform()

    testLogging {
        // showStandardStreams = true  // Un comment this if need full integration test output for stdout & stderr
        exceptionFormat = TestExceptionFormat.FULL
        events("passed", "skipped", "failed")
    }
}


// Set up integration testing -------

// https://docs.gradle.org/current/userguide/java_testing.html#sec:configuring_java_integration_tests

// Define independent source set so we can run separately then unit tests
sourceSets {
    create("intTest") {
        compileClasspath += main.get().output + configurations.testRuntimeClasspath
        runtimeClasspath += output + compileClasspath
    }
}

// Extend base project run time and test configuration then add integration specific deps
val intTestImplementation: Configuration by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
configurations["intTestRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

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