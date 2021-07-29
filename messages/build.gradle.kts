import com.google.protobuf.gradle.*

plugins {
    id("com.google.protobuf") version "0.8.16"
    id("java-library")
    `maven-publish`
    idea
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

group = "client-sdk-java"
version = findProperty("version") as String

var awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID") ?: findProperty("aws_access_key_id") as String? ?: ""
var awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY") ?: findProperty("aws_secret_access_key") as String? ?: ""


repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven {
        name = "messages"
        url = uri("s3://artifact-814370081888-us-west-2/client-sdk-java/release")
        credentials(AwsCredentials::class) {
            accessKey = awsAccessKeyId
            secretKey = awsSecretAccessKey
        }
    }
}


dependencies {

    api("io.grpc:grpc-protobuf:${rootProject.ext["grpcVersion"]}")
    api("io.grpc:grpc-stub:${rootProject.ext["grpcVersion"]}")
    api("com.google.protobuf:protobuf-java-util:${rootProject.ext["protobufVersion"]}")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // necessary for Java 9+

    protobuf(files("src/client_protos/proto/"))

}

protobuf {
    var systemOverride = ""
    if (System.getProperty("os.name") == "Mac OS X") {
        println("overriding protobuf artifacts classifier to osx-x86_64 so M1 Macs can find lib")
        systemOverride = ":osx-x86_64"
    }

    protoc {
        artifact = "com.google.protobuf:protoc:${rootProject.ext["protobufVersion"]}${systemOverride}"
    }

    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${rootProject.ext["grpcVersion"]}${systemOverride}"
        }
    }

    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
            }

        }
    }
}

publishing {
    publications {
        create<MavenPublication>("myLibrary") {
            from(components["java"])
        }
    }

    repositories {
        maven {
            url = uri("s3://artifact-814370081888-us-west-2/client-sdk-java/release")
            // we are using custom creds here so we don't accidentally publish
            credentials(AwsCredentials::class) {
                accessKey = awsAccessKeyId
                secretKey = awsSecretAccessKey
            }
        }
    }
}
