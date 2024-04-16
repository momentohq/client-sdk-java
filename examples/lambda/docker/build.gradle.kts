plugins {
    id("java")
    id("com.github.johnrengelman.shadow") version "7.1.0"
}

group = "com.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.amazonaws:aws-lambda-java-core:1.2.1")
    implementation("software.momento.java:sdk:1.11.0")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "momento.lambda.example"
    }
}

tasks.shadowJar {
    archiveBaseName.set("docker")
    archiveClassifier.set("all")
    mergeServiceFiles()
}