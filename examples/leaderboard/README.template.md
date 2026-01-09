{{ ossHeader }}

# Momento Java SDK - Leaderboard Client Examples

## Running the Examples

- You do not need gradle to be installed
- JDK 14 or above is required to run the example
- To get started with Momento you will need a Momento API key. You can get one from the
  [Momento Console](https://console.gomomento.com).
- A Momento service endpoint is required. Choose the one for the [region](https://docs.momentohq.com/platform/regions) you'll be using, e.g. `cell-1-ap-southeast-1-1.prod.a.momentohq.com` for ap-southeast-1.

### Basic

```bash
MOMENTO_API_KEY=<YOUR API KEY> MOMENTO_ENDPOINT=<YOUR_ENDPOINT> ./gradlew basic
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

{{ ossFooter }}
