<head>
  <meta name="Momento Java Client Library Documentation" content="Java client software development kit for Momento Cache">
</head>
<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-stable.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)

# Momento Java Client Library

Momento Cache is a fast, simple, pay-as-you-go caching solution without any of the operational overhead
required by traditional caching solutions.  This repo contains the source code for the Momento Java client library.

To get started with Momento you will need a Momento Auth Token. You can get one from the [Momento Console](https://console.gomomento.com).

* Website: [https://www.gomomento.com/](https://www.gomomento.com/)
* Momento Documentation: [https://docs.momentohq.com/](https://docs.momentohq.com/)
* Getting Started: [https://docs.momentohq.com/getting-started](https://docs.momentohq.com/getting-started)
* Java SDK Documentation: [https://docs.momentohq.com/develop/sdks/java](https://docs.momentohq.com/develop/sdks/java)
* Discuss: [Momento Discord](https://discord.gg/3HkAKjUZGq)

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
package momento.client.example.doc_examples;

import java.time.Duration;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.GetResponse;

public class ReadmeExample {
  public static void main(String[] args) {
    try (final CacheClient cacheClient =
        CacheClient.create(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            Configurations.Laptop.v1(),
            Duration.ofSeconds(60))) {
      final String cacheName = "cache";

      cacheClient.createCache(cacheName).join();

      cacheClient.set(cacheName, "foo", "bar").join();

      final GetResponse response = cacheClient.get(cacheName, "foo").join();
      if (response instanceof GetResponse.Hit hit) {
        System.out.println("Got value: " + hit.valueString());
      }
    }
  }
}

```

## Getting Started and Documentation

Documentation is available on the [Momento Docs website](https://docs.momentohq.com).

## Examples

Working example projects, with all required build configuration files, are available in the [examples](./examples) subdirectory.

## Developing

If you are interested in contributing to the SDK, please see the [CONTRIBUTING](./CONTRIBUTING.md) docs.

----------------------------------------------------------------------------------------
For more info, visit our website at [https://gomomento.com](https://gomomento.com)!
