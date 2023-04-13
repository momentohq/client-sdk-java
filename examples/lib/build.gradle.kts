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
    implementation("software.momento.java:sdk:0.24.0")

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

task("withDatabase", JavaExec::class) {
    description = "Run the with-database example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.advanced.WithDatabaseExample")
}

task("list", JavaExec::class) {
    description = "Run the list example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.ListExample")
}

task("set", JavaExec::class) {
    description = "Run the set example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.SetExample")
}

task("dictionary", JavaExec::class) {
    description = "Run the dictionary example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.DictionaryExample")
}

task("sortedSet", JavaExec::class) {
    description = "Run the sorted set example"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("momento.client.example.SortedSetExample")
}


task("prepareKotlinBuildScriptModel") {}
