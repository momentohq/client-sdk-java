<head>
  <meta name="Momento Java Client Library Documentation" content="Java client software development kit for Momento Serverless Cache">
</head>
<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-experimental.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md) 

# Momento Java Client Library


:warning: Experimental SDK :warning:

This is an official Momento SDK, but the API is in an early experimental stage and subject to backward-incompatible
changes.  For more info, click on the experimental badge above.


Java client SDK for Momento Serverless Cache: a fast, simple, pay-as-you-go caching solution without
any of the operational overhead required by traditional caching solutions!



## Getting Started :running:

### Requirements

- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)
- At least the java 8 run time installed
- mvn or gradle for downloading the sdk

### Examples

Ready to dive right in? Just check out the [examples](./examples/README.md) directory for complete, working examples of
how to use the SDK.

### Installation

Gradle

Add our mvn repository to your `gradle.build.kts` file and sdk as a dependency

```kotlin
buildscript {
    repositories {
        mavenCentral()
        
        maven("https://momento.jfrog.io/artifactory/maven-public")
    }

    dependencies {
            implementation("momento.sandbox:momento-sdk:0.20.0")
    }
}
```

Maven

Add our mvn repository to your `pom.xml` file and sdk as a dependency

```xml

<project>
    ...
    <repositories>
        <repository>
            <id>momento-sdk</id>
            <url>https://momento.jfrog.io/artifactory/maven-public</url>
        </repository>
    </repositories>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>momento.sandbox</groupId>
                <artifactId>momento-sdk</artifactId>
                <version>0.20.0</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
    ...
</project>
```

### Usage

Checkout our [examples](./examples/README.md) directory for complete examples of how to use the SDK.

Here is a quickstart you can use in your own project:

```kotlin
package momento.client.example;

import java.time.Duration;
import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.ListCachesResponse;

public class MomentoCacheApplication {

  private static final String MOMENTO_AUTH_TOKEN = System.getenv("MOMENTO_AUTH_TOKEN");
  private static final String CACHE_NAME = "cache";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  public static void main(String[] args) {
    printStartBanner();
    try (final CacheClient cacheClient =
        CacheClient.builder(MOMENTO_AUTH_TOKEN, DEFAULT_ITEM_TTL).build()) {

      createCache(cacheClient, CACHE_NAME);

      listCaches(cacheClient);

      System.out.printf("Setting key '%s', value '%s'%n", KEY, VALUE);
      cacheClient.set(CACHE_NAME, KEY, VALUE).join();

      System.out.printf("Getting value for key '%s'%n", KEY);

      final CacheGetResponse getResponse = cacheClient.get(CACHE_NAME, KEY).join();
      if (getResponse instanceof CacheGetResponse.Hit hit) {
        System.out.printf("Found value for key '%s': '%s'%n", KEY, hit.valueString());
      } else if (getResponse instanceof CacheGetResponse.Miss) {
        System.out.println("Found no value for key " + KEY);
      } else if (getResponse instanceof CacheGetResponse.Error error) {
        System.out.printf("Error occurred when looking up value for key '%s':%n", KEY);
        System.out.println(error.getMessage());
      }
    }
    printEndBanner();
  }

  private static void createCache(CacheClient cacheClient, String cacheName) {
    try {
      cacheClient.createCache(cacheName);
    } catch (AlreadyExistsException e) {
      System.out.printf("Cache with name '%s' already exists.%n", cacheName);
    }
  }

  private static void listCaches(CacheClient cacheClient) {
    System.out.println("Listing caches:");
    final ListCachesResponse listCachesResponse = cacheClient.listCaches();
    if (listCachesResponse instanceof ListCachesResponse.Success success) {
      for (CacheInfo cacheInfo : success.getCaches()) {
        System.out.println(cacheInfo.name());
      }
    } else if (listCachesResponse instanceof ListCachesResponse.Error error) {
      System.out.println("Error occurred listing caches:");
      System.out.println(error.getMessage());
    }
  }

  private static void printStartBanner() {
    System.out.println("******************************************************************");
    System.out.println("*                      Momento Example Start                     *");
    System.out.println("******************************************************************");
  }

  private static void printEndBanner() {
    System.out.println("******************************************************************");
    System.out.println("*                       Momento Example End                      *");
    System.out.println("******************************************************************");
  }
}

```

### Error Handling

Coming soon

### Tuning

Coming soon

----------------------------------------------------------------------------------------
For more info, visit our website at [https://gomomento.com](https://gomomento.com)!
