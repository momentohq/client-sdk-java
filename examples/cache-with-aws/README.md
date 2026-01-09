# Java Client SDK

<br>

## Running the Example

This demo is similar to the Basic demo under the ```cache``` directory where it creates a cache, lists
all the existing caches, and sets and gets a key. However, it fetches your Momento authentication token
from AWS secrets manager rather than the environment variable. 

- You do not need gradle to be installed
- JDK 14 or above is required to run the example
- To get started with Momento you will need a Momento API Key. You can get one from the 
  [Momento Console](https://console.gomomento.com).
  - As a pre-requisite, you need to store your
Momento API Key in AWS Secrets Manager. Refer to our [documentation](https://docs.momentohq.com/develop/integrations/aws-secrets-manager) 
for a walkthrough of storing the token in Secrets Manager.
- A Momento service endpoint is required. Choose the one for the [region](https://docs.momentohq.com/platform/regions) you'll be using and set it as the value of the `MOMENTO_ENDPOINT` variable. Alternately, store it as an AWS Secret like for the API key and retrieve it in the same way.
- Make sure to configure your AWS credentials in order to use the [DefaultCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html), or set up a different provider.

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
    implementation("software.momento.java:sdk:1.23.0")
    implementation("software.amazon.awssdk:secretsmanager:2.20.93")
}
```
