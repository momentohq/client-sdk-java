/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java library project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.2/userguide/building_java_projects.html
 */

plugins {
    application
    id("com.diffplug.spotless") version "5.15.1"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    implementation("software.momento.java:sdk:1.7.0")

    implementation("com.google.guava:guava:31.1-android")

    // Logging framework to log and enable logging in the Momento client.
    implementation("ch.qos.logback:logback-classic:1.4.7")

    // Histogram for collecting stats in the load generator
    implementation("org.hdrhistogram:HdrHistogram:2.1.12")

    // Use JUnit Jupiter for testing.
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
}

spotless {
    java {
        removeUnusedImports()
        googleJavaFormat("1.11.0")
    }
}

tasks.test {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}

task("disposableToken", JavaExec::class) {
    description = "Run the disposable token example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.DisposableTokenExample")
}

task("prepareKotlinBuildScriptModel") {}
