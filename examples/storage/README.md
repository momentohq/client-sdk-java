<img src="https://docs.momentohq.com/img/momento-logo-forest.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-beta.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)


# Momento Java SDK - Storage Client Examples

## Running the Examples

- You do not need gradle to be installed
- JDK 14 or above is required to run the example
- To get started with Momento you will need a Momento API key. You can get one from the
  [Momento Console](https://console.gomomento.com).

### Basic

```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew basic
```

Example Code: [BasicExample.java](src/main/java/momento/client/example/BasicExample.java)

## Using the Java SDK in your project

### Gradle Configuration

Update your Gradle build to include the components

#### build.gradle.kts

```kotlin
dependencies {
    implementation("software.momento.java:sdk:1.x.x")
}
```

----------------------------------------------------------------------------------------
For more info, visit our website at [https://gomomento.com](https://gomomento.com)!
