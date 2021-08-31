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
    * `TEST_AUTH_TOKEN=<auth token> TEST_ENDPOINT=<endpoint> ./gradlew integrationTest`
      * `TEST_ENDPOINT` is optional and defaults to alpha. TEST_ENDPOINT must belong to the cell where the auth token was generated.
   
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
