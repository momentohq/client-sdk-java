# Java Client SDK

_Read this in other languages_: [日本語](README.ja.md)

<br>

## Running the Examples

- You do not need gradle to be installed
- JDK 14 or above is required to run the example
- To get started with Momento you will need a Momento API key. You can get one from the
  [Momento Console](https://console.gomomento.com).

### Basic
```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew basic
```

Example Code: [BasicExample.java](cache/src/main/java/momento/client/example/BasicExample.java)


### List
```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew list
```

Example Code: [ListExample.java](cache/src/main/java/momento/client/example/ListExample.java)

### Set
```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew set
```

Example Code: [SetExample.java](cache/src/main/java/momento/client/example/SetExample.java)

### Dictionary
```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew dictionary
```

Example Code: [DictionaryExample.java](cache/src/main/java/momento/client/example/DictionaryExample.java)

### Sorted Set
```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew sortedSet
```

Example Code: [SortedSetExample.java](cache/src/main/java/momento/client/example/SortedSetExample.java)

### Batch Util
```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew sortedSet
```

Example Code: [SortedSetExample.java](cache/src/main/java/momento/client/example/SortedSetExample.java)


### With a Backing Database
```bash
MOMENTO_API_KEY=<YOUR API KEY> ./gradlew withDatabase
```

Example Code: [WithDatabaseExample.java](cache/src/main/java/momento/client/example/advanced/WithDatabaseExample.java)

## Using the Java SDK in your project

### Gradle Configuration

Update your Gradle build to include the components

**build.gradle.kts**

```kotlin
dependencies {
    implementation("software.momento.java:sdk:0.24.0")
}
```
