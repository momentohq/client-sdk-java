repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    implementation(kotlin("gradle-plugin"))
    implementation("org.jfrog.buildinfo:build-info-extractor-gradle:4.24.16")
}

plugins {
    kotlin("jvm") version "1.5.21"
    `kotlin-dsl`
}
