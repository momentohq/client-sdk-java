plugins {
    id("momento.artifactory-java-lib")
    id("momento.junit-tests")
    id("momento.integration-tests")
    id("com.diffplug.spotless") version "5.15.1"
}

val opentelemetryVersion = rootProject.ext["opentelemetryVersion"]
val jwtVersion = rootProject.ext["jwtVersion"]
val grpcVersion = rootProject.ext["grpcVersion"]
val guavaVersion = rootProject.ext["guavaVersion"]

dependencies {
    implementation(platform("io.opentelemetry:opentelemetry-bom:$opentelemetryVersion"))
    implementation("io.opentelemetry:opentelemetry-api")
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("com.google.guava:guava:$guavaVersion")
    implementation("com.google.code.gson:gson:2.8.9")
    implementation("com.google.protobuf:protobuf-java:3.21.2")
    implementation("io.grpc:grpc-api:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.opentelemetry:opentelemetry-context:$opentelemetryVersion")

    // For Auth token
    implementation("io.jsonwebtoken:jjwt-api:$jwtVersion")
    runtimeOnly("io.jsonwebtoken:jjwt-impl:$jwtVersion")
    runtimeOnly("io.jsonwebtoken:jjwt-jackson:$jwtVersion")

    // Internal Deps -------------------
    implementation("software.momento.java:client-protos:0.54.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.assertj:assertj-core:3.24.2")
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
