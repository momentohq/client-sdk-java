plugins {
    id("momento.publishable-java-lib")
    id("momento.junit-tests")
    id("momento.integration-tests")
    id("com.diffplug.spotless") version "5.15.1"
}

dependencies {
    implementation(libs.momento.java.protos)

    api(libs.jsr305) // Marked api because the annotations are used in sdk methods
    api(libs.grpc.api) // Marked api because SdkException contains classes from this dependency
    implementation(libs.grpc.stub)
    implementation(libs.grpc.nettyshaded)
    implementation(libs.protobuf.java)
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.slf4j.api)

    // For Auth token
    implementation(libs.jjwt.api)
    runtimeOnly(libs.jjwt.impl)
    runtimeOnly(libs.jjwt.gson)

    // Test dependencies
    testImplementation(libs.junit)
    testImplementation(libs.assertj)
}

spotless {
    java {
        removeUnusedImports()
        googleJavaFormat("1.11.0")
    }
}

// Modifying the JAR manifest so that can access release version in code
tasks.withType<Jar> {
    manifest {
        attributes["Implementation-Version"] = findProperty("version") as String
    }
}

// Disable dependency checking for tests because the plugin does not play well with our unusual test structure
tasks.named("analyzeIntTestClassesDependencies").configure {
    enabled = false
}
tasks.named("analyzeTestClassesDependencies").configure {
    enabled = false
}
