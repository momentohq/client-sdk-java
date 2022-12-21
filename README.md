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

import java.util.Optional;
import momento.sdk.SimpleCacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.ListCachesResponse;

public class MomentoCacheApplication {

  private static final String MOMENTO_AUTH_TOKEN = System.getenv("MOMENTO_AUTH_TOKEN");
  private static final String CACHE_NAME = "cache";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final int DEFAULT_ITEM_TTL_SECONDS = 60;

  public static void main(String[] args) {
    printStartBanner();
    try (SimpleCacheClient simpleCacheClient =
        SimpleCacheClient.builder(MOMENTO_AUTH_TOKEN, DEFAULT_ITEM_TTL_SECONDS).build()) {

      createCache(simpleCacheClient, CACHE_NAME);

      listCaches(simpleCacheClient);

      System.out.println(String.format("Setting key=`%s` , value=`%s`", KEY, VALUE));
      simpleCacheClient.set(CACHE_NAME, KEY, VALUE);

      System.out.println(String.format("Getting value for key=`%s`", KEY));

      CacheGetResponse getResponse = simpleCacheClient.get(CACHE_NAME, KEY);
      System.out.println(String.format("Lookup resulted in: `%s`", getResponse.status()));
      System.out.println(
          String.format("Looked up value=`%s`", getResponse.string().orElse("NOT FOUND")));
    }
    printEndBanner();
  }

  private static void createCache(SimpleCacheClient simpleCacheClient, String cacheName) {
    try {
      simpleCacheClient.createCache(cacheName);
    } catch (AlreadyExistsException e) {
      System.out.println(String.format("Cache with name `%s` already exists.", cacheName));
    }
  }

  private static void listCaches(SimpleCacheClient simpleCacheClient) {
    System.out.println("Listing caches:");
    Optional<String> token = Optional.empty();
    do {
      ListCachesResponse listCachesResponse = simpleCacheClient.listCaches(token);
      for (CacheInfo cacheInfo : listCachesResponse.caches()) {
        System.out.println(cacheInfo.name());
      }
      token = listCachesResponse.nextPageToken();
    } while (token.isPresent());
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
