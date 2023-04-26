{{ ossHeader }}

## Getting Started :running:

### Requirements

- A Momento Auth Token is required, you can generate one using
  the [Momento CLI](https://github.com/momentohq/momento-cli)
- At least the java 8 run time installed
- mvn or gradle for downloading the sdk

### Examples

Ready to dive right in? Just check out the [examples](./examples/README.md) directory for complete, working examples of
how to use the SDK.

### Installation

#### Gradle

Add our dependency to your `gradle.build.kts`

```kotlin
buildscript {
    dependencies {
        implementation("software.momento.java:sdk:0.24.0")
    }
}
```

#### Maven

Add our dependency your `pom.xml`

```xml

<project>
    ...
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>software.momento.java</groupId>
                <artifactId>sdk</artifactId>
                <version>0.24.0</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
    ...
</project>
```

### Usage

Checkout our [examples](./examples/README.md) directory for complete examples of how to use the SDK.

Here is a basic example you can use to get started:

```java
{{usageExampleCode}}
```

### Error Handling

The SDK will only throw exceptions from errors encountered when setting up a client. All errors that occur when calling
the client methods will result in an error response. All methods have an `Error` response subclass alongside the other
response types they can return.

Here is an example of how the response can be matched to different outcomes:

```java
final ListFetchResponse fetchResponse=client.listFetch(...).join();
if (fetchResponse instanceof ListFetchResponse.Hit hit) {
  // A successful call that returned a result.
} else if(fetchResponse instanceof ListFetchResponse.Miss miss) {
  // A successful call that didn't find anything
}else if(fetchResponse instanceof ListFetchResponse.Error error) {
  // An error result. It is an exception and can be thrown if desired.
}
```

### Tuning

SDK tuning is done through the Configuration object passed into the client builder. Preset Configuration objects for
different environments are defined
in [Configurations](momento-sdk/src/main/java/momento/sdk/config/Configurations.java).

{{ ossFooter }}
