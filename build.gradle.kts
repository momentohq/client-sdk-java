ext["grpcVersion"] = "1.39.0"
ext["protobufVersion"] = "3.17.3"
ext["opentelemetryVersion"] = "1.4.1"

val currentVersion: String by project

allprojects {
    // These fields are used by the artifactoryPublish task
    //  to determine the artifact group ID and version that
    //  customers will consume.
    group = "momento.sandbox"
    version = currentVersion
}
