plugins {
    id("ca.cutterslade.analyze") version "1.9.0"
}

allprojects {
    // These fields are used by the artifactoryPublish task
    //  to determine the artifact group ID and version that
    //  customers will consume.
    group = "momento.sandbox"
    version = findProperty("version") as String
    apply(plugin = "ca.cutterslade.analyze")
}

// Spotless plugin used for Java Formatting needs the buildscript with repository
// to be defined in the ROOT_PROJECT
// https://github.com/diffplug/spotless/issues/747
buildscript {
    repositories {
        mavenCentral()
    }
}
