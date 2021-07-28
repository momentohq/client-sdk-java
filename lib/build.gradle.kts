import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    `java-library`
    `maven-publish`
    idea
    java
}

group = "org.momento"
version = findProperty("version") as String

var awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID") ?: findProperty("aws_access_key_id") as String? ?: ""
var awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY") ?: findProperty("aws_secret_access_key") as String? ?: ""

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven {
        name = ""
        url = uri("s3://artifact-814370081888-us-west-2/client-sdk-java/release")
        credentials(AwsCredentials::class) {
            accessKey = awsAccessKeyId
            secretKey = awsSecretAccessKey
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
            credentials(AwsCredentials::class) {
                accessKey = awsAccessKeyId
                secretKey = awsSecretAccessKey
            }
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")

    // Internal Deps --------------------
    implementation("io.grpc:grpc-netty:${rootProject.ext["grpcVersion"]}")
    implementation(project(":messages"))
}

tasks.test {
    useJUnitPlatform()

    testLogging {
        // showStandardStreams = true  // Un comment this if need full integration test output for stdout & stderr
        exceptionFormat = TestExceptionFormat.FULL
        events("passed", "skipped", "failed")
    }
}
