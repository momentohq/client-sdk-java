# Java Client SDK

## Examples

All examples are in subdirectories of `examples/cache` and `examples/cache-with-aws`. We have different directories for different examples in order to keep the `build.gradle.kts` files to a minimum for users who may be copying things from them. For instance, when we have examples that use the AWS SDK, we don't want users to think they need those deps for a basic momento project, so we put them in their own directory with their own ```build.gradle.kts```.

All examples specify an explicit dependency on a released version of the SDK. We use dependabot to detect when there are new SDK releases and automatically file a PR to update the examples; this ensures that our CI is testing the actual packages from the maven repository.

When you are adding a new examples directory, add it to the dependabot config here:

https://github.com/momentohq/client-sdk-java/blob/main/.github/dependabot.yml#L6-#L13