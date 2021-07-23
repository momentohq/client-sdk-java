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

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")

    // Internal Deps -------------------
    implementation("io.grpc:grpc-netty:${rootProject.ext["grpcVersion"]}")
    implementation(project(":messages"))
}
