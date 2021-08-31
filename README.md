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
    * Generate test auth token as per TODO TODO TODO 
    * `TEST_AUTH_TOKEN=<auth token> ./gradlew integrationTest`
   
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
