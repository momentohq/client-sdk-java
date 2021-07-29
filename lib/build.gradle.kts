import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    `java-library`
    idea
    java
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

val opentelemetryVersion = rootProject.ext["opentelemetryVersion"]

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