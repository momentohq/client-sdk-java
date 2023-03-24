plugins {
    id("ca.cutterslade.analyze") version "1.9.0"
    id("io.github.gradle-nexus.publish-plugin") version "1.3.0"
}

group = "software.momento.java"

allprojects {
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

// Only configure the nexus publishing plugin if we have credentials and the version is publishable.
private val sonatypeUsername: String? = System.getenv("SONATYPE_USERNAME")
private val sonatypePassword: String? = System.getenv("SONATYPE_PASSWORD")
if (sonatypeUsername != null && sonatypePassword != null) {
    if (version.toString() != "unspecified" && !version.toString().endsWith("SNAPSHOT")) {
        nexusPublishing {
            repositories {
                sonatype {
                    nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
                    snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
                    username.set(System.getenv("SONATYPE_USERNAME"))
                    password.set(System.getenv("SONATYPE_PASSWORD"))
                }
            }
        }
    }
}
