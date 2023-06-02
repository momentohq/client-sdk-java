{{ ossHeader }}

## Packages

The Java SDK is available on Maven Central:

### Gradle

```kotlin
implementation("software.momento.java:sdk:1.0.0")
```

### Maven

```xml
<dependency>
    <groupId>software.momento.java</groupId>
    <artifactId>sdk</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

```java
{% include "./examples/lib/src/main/java/momento/client/example/doc_examples/ReadmeExample.java" %}
```

## Getting Started and Documentation

Documentation is available on the [Momento Docs website](https://docs.momentohq.com).

## Examples

Working example projects, with all required build configuration files, are available in the [examples](./examples) subdirectory.

## Developing

If you are interested in contributing to the SDK, please see the [CONTRIBUTING](./CONTRIBUTING.md) docs.

{{ ossFooter }}
