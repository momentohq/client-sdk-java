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

Here is a quickstart you can use in your own project:

```java
package momento.client.example;

import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.control.CacheInfo;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheListResponse;

public class BasicExample extends AbstractExample {

    private static final String CACHE_NAME = "cache";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    public static void main(String[] args) {
        printStartBanner("Basic");
        try (final CacheClient cacheClient = buildCacheClient()) {

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
        printEndBanner("Basic");
    }

    private static void createCache(CacheClient cacheClient, String cacheName) {
        final CreateCacheResponse createCacheResponse = cacheClient.createCache(cacheName);
        if (createCacheResponse instanceof CreateCacheResponse.Error error) {
            if (error.getCause() instanceof AlreadyExistsException) {
                System.out.println("Cache with name '" + cacheName + "' already exists.");
            } else {
                System.out.println("Unable to create cache with error: " + error.getMessage());
            }
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
}

```

### Error Handling

The SDK will only throw exceptions from errors encountered when setting up a client. All errors that occur when calling
the client methods will result in an error response. All methods have an `Error` response subclass alongside the other
response types they can return.

Here is an example of how the response can be matched to different outcomes:

```java
final CacheListFetchResponse fetchResponse=client.listFetch(...).join();
if (fetchResponse instanceof CacheListFetchResponse.Hit hit) {
  // A successful call that returned a result.
} else if(fetchResponse instanceof CacheListFetchResponse.Miss miss) {
  // A successful call that didn't find anything
}else if(fetchResponse instanceof CacheListFetchResponse.Error error) {
  // An error result. It is an exception and can be thrown if desired.
}
```

### Tuning

SDK tuning is done through the Configuration object passed into the client builder. Preset Configuration objects for
different environments are defined
in [Configurations](momento-sdk/src/main/java/momento/sdk/config/Configurations.java).

----------------------------------------------------------------------------------------
For more info, visit our website at [https://gomomento.com](https://gomomento.com)!
