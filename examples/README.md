# Java Client SDK

## Examples

All examples are in subdirectories of `examples/cache` and `examples/cache-with-aws`. We have different directories for different examples in order to keep the `build.gradle.kts` files to a minimum for users who may be copying things from them. For instance, when we have examples that use the AWS SDK, we don't want users to think they need those deps for a basic momento project, so we put them in their own directory with their own ```build.gradle.kts```.

- Examples to get started with Momento: 
  - https://github.com/momentohq/client-sdk-java/tree/main/examples/cache
- Examples that uses Momento with one or more AWS integrations, such as AWS Secrets Manager to store your Momento auth token:
  - https://github.com/momentohq/client-sdk-java/tree/main/examples/cache-with-aws