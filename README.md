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
    * `TEST_AUTH_TOKEN=<auth token> gradle clean build`