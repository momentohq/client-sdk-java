// Spotless plugin used for Java Formatting needs the buildscript with repository
// to be defined in the ROOT_PROJECT
// https://github.com/diffplug/spotless/issues/747
buildscript {
    repositories {
        mavenCentral()
    }
}
