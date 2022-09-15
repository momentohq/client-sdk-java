# Java Client SDK

_Read this in other languages_: [日本語](README.ja.md)

<br>

## Running the Example

- You do not need gradle to be installed
- JDK 11 or above is required to run the example
- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)

```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew run
```

Example Code: [MomentoApplication.java](lib/src/main/java/momento/client/example/MomentoCacheApplication.java)

## Using the Java SDK in your project

### Gradle Configuration

Update your Gradle build to include the components

**build.gradle.kts**

```kotlin
repositories {
    maven("https://momento.jfrog.io/artifactory/maven-public")
}

dependencies {
    implementation("momento.sandbox:momento-sdk:0.18.0")
}
```
