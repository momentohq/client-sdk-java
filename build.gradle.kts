ext["grpcVersion"] = "1.47.0"
ext["protobufVersion"] = "3.21.2"
ext["opentelemetryVersion"] = "1.5.0"
ext["jwtVersion"] = "0.11.2"

allprojects {
    // These fields are used by the artifactoryPublish task
    //  to determine the artifact group ID and version that
    //  customers will consume.
    group = "momento.sandbox"
    version = findProperty("version") as String
}

// Spotless plugin used for Java Formatting needs the buildscript with repository
// to be defined in the ROOT_PROJECT
// https://github.com/diffplug/spotless/issues/747
buildscript {
    repositories {
        mavenCentral()
    }
}
