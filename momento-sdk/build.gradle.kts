plugins {
    id("momento.artifactory-java-lib")
    id("momento.junit-tests")
    id("momento.integration-tests")
    id("com.diffplug.spotless") version "5.15.1"
}

val opentelemetryVersion = rootProject.ext["opentelemetryVersion"]
val jwtVersion = rootProject.ext["jwtVersion"]

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("commons-io:commons-io:2.11.0")

    implementation(platform("io.opentelemetry:opentelemetry-bom:$opentelemetryVersion"))
    implementation("io.opentelemetry:opentelemetry-api")
    implementation("io.grpc:grpc-netty-shaded:${rootProject.ext["grpcVersion"]}")

    // For Auth token
    implementation("io.jsonwebtoken:jjwt-api:$jwtVersion")
    runtimeOnly("io.jsonwebtoken:jjwt-impl:$jwtVersion")
    runtimeOnly("io.jsonwebtoken:jjwt-jackson:$jwtVersion")

    // Internal Deps -------------------
    implementation(project(":messages"))

    implementation("org.apache.commons:commons-lang3:3.0")

    testImplementation("io.opentelemetry:opentelemetry-sdk:$opentelemetryVersion")
    testImplementation("io.opentelemetry:opentelemetry-exporter-otlp:$opentelemetryVersion")
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
