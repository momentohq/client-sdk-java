# Java Client SDK

_Read this in other languages_: [日本語](README.ja.md)

<br>

## Running the Examples

- You do not need gradle to be installed
- JDK 14 or above is required to run the example
- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)

### Basic
```bash
./gradlew basic-aws
```
Example Code: [BasicExample.java](cache-with-aws/src/main/java/momento/client/example/BasicExample.java)

### Gradle

Update your Gradle build to add the component to your project.

**build.gradle.kts**

```kotlin
dependencies {
    implementation("software.momento.java:sdk:0.24.0")
    implementation("software.amazon.awssdk:secretsmanager:2.20.93")
}
```