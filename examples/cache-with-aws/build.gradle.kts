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
    implementation("software.momento.java:sdk:1.21.0")

    // For examples to store secrets in AWS Secrets Manager
    implementation("software.amazon.awssdk:secretsmanager:2.20.93")

    // Logging framework to log and enable logging in the Momento client.
    implementation("ch.qos.logback:logback-classic:1.4.7")

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

task("basic-aws", JavaExec::class) {
    description = "Run the basic example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.BasicExample")
}

task("prepareKotlinBuildScriptModel") {}
