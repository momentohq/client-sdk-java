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
    implementation("software.momento.java:sdk:1.15.1")

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

task("basic", JavaExec::class) {
    description = "Run the basic example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.BasicExample")
}

task("docExamples", JavaExec::class) {
    description = "Validate that the API doc examples run"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.doc_examples.DocExamplesJavaAPIs")
}

task("docCheatSheet", JavaExec::class) {
    description = "Validate that the doc cheat sheet runs"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.doc_examples.CheatSheet")
}

task("docsTasks") {
    dependsOn("docCheatSheet")
    dependsOn("docExamples")
}

task("prepareKotlinBuildScriptModel") {}
