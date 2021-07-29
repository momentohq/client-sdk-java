# client-sdk-java
Java sdk customers can use to interact with our ecosystem

## Development

1. Install Java
   * `brew install openjdk`
1. Install Gradle
   * `brew install gradle`
1. Clone repo
    * `git clone git@github.com:momentohq/client-sdk-java.git`
1. Initialize git submodule
    * `git submodule init && git submodule update`
1. Run gradle build
    * `gradle clean build`
   
## How to import into your project
Add this to your `gradle.build.kts` file
```
var awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID") ?: findProperty("aws_access_key_id") as String? ?: "NONE"
var awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY") ?: findProperty("aws_secret_access_key") as String? ?: "NONE"

repositories {
    maven {
        url = uri("s3://artifact-814370081888-us-west-2/client-sdk-java/release")
        credentials(AwsCredentials::class) {
            accessKey = awsAccessKeyId
            secretKey = awsSecretAccessKey
        }
    }
}
```
Then you can either 
- set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- or run
`gradle clean build -Paws_access_key_id=YOUR_ACCESS_KEY -Paws_secret_access_key=YOUR_SECRET_KEY`
