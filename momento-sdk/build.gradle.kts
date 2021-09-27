plugins {
    id("momento.artifactory-java-lib")
    id("momento.junit-tests")
    id("momento.integration-tests")
    id("com.diffplug.spotless") version "5.15.1"
}

val opentelemetryVersion = rootProject.ext["opentelemetryVersion"]

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")

    platform("io.opentelemetry:opentelemetry-bom:$opentelemetryVersion")
    implementation("io.opentelemetry:opentelemetry-api:$opentelemetryVersion")
    implementation("io.opentelemetry:opentelemetry-sdk:$opentelemetryVersion")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:$opentelemetryVersion")
    implementation("io.grpc:grpc-netty:${rootProject.ext["grpcVersion"]}")

    // Internal Deps -------------------
    implementation(project(":messages"))
}

spotless {
    java {
        removeUnusedImports()
        googleJavaFormat("1.11.0")
    }
}
