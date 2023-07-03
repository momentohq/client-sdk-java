# Java Client SDK

<br>

## Running the Examples

- You do not need gradle to be installed
- JDK 14 or above is required to run the example
- To get started with Momento you will need a Momento Auth Token. You can get one from the 
  [Momento Console](https://console.gomomento.com).
- This demo is similar to the Basic demo under the ```cache``` directory where it creates a cache, lists
all the existing caches, and sets and gets a key. However, it fetches your Momento authentication token
from AWS secrets manager rather than the environment variable. As a pre-requisite, you need to store your
Momento auth token retrieved from the console in AWS Secrets Manager. Refer to our [documentation](https://docs.momentohq.com/develop/integrations/aws-secrets-manager) 
for a walkthrough of storing the token in Secrets Manager.

### Basic
```bash
./gradlew basic-aws
```
Example Code: [BasicExample.java](cache-with-aws/src/main/java/momento/client/example/BasicExample.java)

### Gradle

Update your Gradle build to add the dependencies to your project.

**build.gradle.kts**

```kotlin
dependencies {
    implementation("software.momento.java:sdk:0.24.0")
    implementation("software.amazon.awssdk:secretsmanager:2.20.93")
}
```