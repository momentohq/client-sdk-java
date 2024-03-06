## Development

1. Install Java
    * `brew install openjdk`
1. Clone repo
    * `git clone git@github.com:momentohq/client-sdk-java.git`
1. Run gradle build
    * `./gradlew clean build`
1. To run integration tests:
    * Generate API key using the [Momento Console](https://console.gomomento.com/api-keys) (if you don't already have one)
    * `TEST_AUTH_TOKEN=<api key> TEST_CACHE_NAME=<cache id> TEST_ENDPOINT=<endpoint> ./gradlew integrationTest`
        * `TEST_CACHE_NAME` is required. Give it any string value for now. TODO - Add a way of getting this per environment
        * `TEST_ENDPOINT` is optional and defaults to alpha. TEST_ENDPOINT must belong to the cell where the auth token was generated.
      
### Code Formatting
[google-java-format](https://github.com/google/google-java-format) is used for code formatting.

If you use IntelliJ, follow [instructions] https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides

If the Java code isn't formatted correctly, then your build will fail.

To fix the formatting either reformat the code using the IDE based plugin or run:

`./gradlew :momento-sdk:spotlessApply`

### Examples

The example code can be found in the `examples` directory. If you would like to run the examples against your local copy of the SDK source code, uncomment the `includeBuild` stanza in the `examples/settings.gradle.kts` file. This will allow you to test the example code against local changes that you have made to the SDK.
