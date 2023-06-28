# Java Client SDK

_Read this in other languages_: [日本語](README.ja.md)

<br>

## Running the Examples

- You do not need gradle to be installed
- JDK 14 or above is required to run the example
- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)

### Basic
```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew basic
```
Example Code: [BasicExample.java](lib/src/main/java/momento/client/example/BasicExample.java)


### List
```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew list
```
Example Code: [ListExample.java](lib/src/main/java/momento/client/example/ListExample.java)

### Set
```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew set
```
Example Code: [SetExample.java](lib/src/main/java/momento/client/example/SetExample.java)

### Dictionary
```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew dictionary
```
Example Code: [DictionaryExample.java](lib/src/main/java/momento/client/example/DictionaryExample.java)

### Sorted Set
```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew sortedSet
```
Example Code: [SortedSetExample.java](lib/src/main/java/momento/client/example/SortedSetExample.java)


### With a Backing Database
```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew withDatabase
```
Example Code: [WithDatabaseExample.java](lib/src/main/java/momento/client/example/advanced/WithDatabaseExample.java)

## Using the Java SDK in your project

### Gradle Configuration

Update your Gradle build to include the components

**build.gradle.kts**

```kotlin
dependencies {
    implementation("software.momento.java:sdk:0.24.0")
}
```
