plugins {
    `java-library`
    `maven-publish`
    `signing`
}

repositories {
    mavenCentral()
}

java {
    withSourcesJar()
    withJavadocJar()
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

// Sign only if we have a key to do so
val signingKey: String? = System.getenv("SONATYPE_SIGNING_KEY")
if (signingKey != null) {
    signing {
        useInMemoryPgpKeys(signingKey, System.getenv("SONATYPE_SIGNING_KEY_PASSWORD"))
        sign(publishing.publications["mavenJava"])
    }
}

publishing {
    publications {
        register<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = rootProject.group.toString()
            artifactId = project.name.toString().substringAfter("momento-")

            pom {
                name.set("Momento Java SDK")
                description.set("Java client SDK for Momento Serverless Cache")
                url.set("https://github.com/momentohq/client-sdk-java")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("momento")
                        name.set("Momento")
                        organization.set("Momento")
                        email.set("eng-deveco@momentohq.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/momentohq/client-sdk-java.git")
                    developerConnection.set("scm:git:git@github.com:momentohq/client-sdk-java.git")
                    url.set("https://github.com/momentohq/client-sdk-java")
                }
            }
        }
    }
}
