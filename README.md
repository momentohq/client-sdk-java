# client-sdk-java

:warning: Experimental SDK :warning:

Java SDK for Momento is experimental and under active development. There could be non-backward compatible changes or
removal in the future. Please be aware that you may need to update your source code with the current version of the SDK
when its version gets upgraded.
---

<br />
Java SDK for Momento, a serverless cache that automatically scales without any of the operational overhead required by
traditional caching solutions.

<br/>

# Getting Started :running:

## Requirements

- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)
- At least the java 8 run time installed
- mvn or gradle for downloading the sdk

## Using Momento

Check out full working code in our [Java SDK example repo](https://github.com/momentohq/client-sdk-examples/tree/main/java)!

### Import into your project

#### Gradle

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

#### Maven

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
## Using Momento

```java
public class MomentoCacheApplication {
    private static final String MOMENTO_AUTH_TOKEN = System.getenv("MOMENTO_AUTH_TOKEN");
    private static final int DEFAULT_ITEM_TTL_SECONDS = 60;

    public static void main(String[] args) {
        // Initializing Momento
        try (SimpleCacheClient simpleCacheClient = SimpleCacheClient
                .builder(MOMENTO_AUTH_TOKEN, DEFAULT_ITEM_TTL_SECONDS)
                .build()) {

            // Creating a cache named "my_first_cache"
            simpleCacheClient.createCache("my_first_cache");

            // Sets key with default TTL and get value with that key
            simpleCacheClient.set("my_first_cache", "my_key", "my_value");
            CacheGetResponse getResponse = simpleCacheClient.get("my_first_cache", "my_key");
            System.out.println(
                    String.format("Looked up value=`%s`", getResponse.string().orElse("NOT FOUND")));
        }
    }
}
```

## Contributing

If you would like to contribute to the Momento Java SDK, please read our [Contributing Guide](./CONTRIBUTING.md)
