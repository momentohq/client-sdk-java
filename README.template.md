{{ ossHeader }}

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
{{ usageExampleCode }}
```

### Error Handling

Coming soon

### Tuning

Coming soon

{{ ossFooter }}
