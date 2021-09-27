# client-sdk-java
Java sdk customers can use to interact with our ecosystem

## Development

1. Install Java
   * `brew install openjdk`
1. Clone repo
    * `git clone git@github.com:momentohq/client-sdk-java.git`
1. Initialize git submodule
    * `git submodule init && git submodule sync && git submodule update`
1. Run gradle build
    * `./gradlew clean build`
1. To run integration tests:
    * Generate test auth token (if you don't already have one)
      * Pre-requisites
        * Cell under test must be fully provisioned
        * `mm` must be installed and configured as per [instructions](https://github.com/momentohq/mm#mm)
        * Must have AWS Access Key ID and AWS Secret Key belonging to the Cell under test
      * Run the following command and it will generate and print an `<auth token>`
        * `AWS_ACCESS_KEY_ID=<ACCESS_KEY> AWS_SECRET_ACCESS_KEY=<SECRET_KEY> mm keys generate-api-key <test_key_name> --cell <cell_name>`
    * `TEST_AUTH_TOKEN=<auth token> TEST_CACHE_NAME=<cache id> TEST_ENDPOINT=<endpoint> ./gradlew integrationTest`
      * `TEST_CACHE_NAME` is required. Give it any string value for now. TODO - Add a way of getting this per environment
      * `TEST_ENDPOINT` is optional and defaults to alpha. TEST_ENDPOINT must belong to the cell where the auth token was generated.
### Code Formatting
[google-java-format](https://github.com/google/google-java-format) is used for code formatting.

If you use your IntelliJ, follow [instructions] https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides

If the Java code isn't formatted correctly, then your build will fail. 

To fix the formatting either reformat the code using the IDE based plugin or run:

`./gradlew :momento-sdk:spotlessApply`

## How to import into your project
Add this to your `gradle.build.kts` file
```
buildscript {
    repositories {
        mavenCentral()
    }

    dependencies {
        classpath("com.amazonaws:aws-java-sdk:1.12.32")
    }
}

repositories {
    maven {
        url = uri("s3://artifact-814370081888-us-west-2/client-sdk-java/release")
        credentials(AwsCredentials::class) {
            val defaultCredentials = com.amazonaws.auth.DefaultAWSCredentialsProviderChain().getCredentials()
            accessKey = defaultCredentials.awsAccessKeyId
            secretKey = defaultCredentials.awsSecretKey
        }
    }
}
```
Then you should be able to use your aws credentials and build your app with the sdk
