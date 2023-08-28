<head>
  <meta name="Momento Java Client Library Documentation" content="Java client software development kit for Momento Cache">
</head>
<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-stable.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)

<br>

## Example Lambda

This directory contains an example lambda, built using AWS CDK, that performs a basic set and get operation on Momento cache.

The primary use is to provide a base for testing Momento in an AWS lambda environment. The lambda creates a Momento client, and then calls a set and get on a hard-coded key/value pair.

## Prerequisites

- Node version 14 or higher is required (for deploying the Cloudformation stack containing the Lambda)
- Gradle https://gradle.org/install/
- To get started with Momento you will need a Momento Auth Token. You can get one from the [Momento Console](https://console.gomomento.com). Check out the [getting started](https://docs.momentohq.com/getting-started) guide for more information on obtaining an auth token.

## Deploying the Momento Python Lambda

First let's build our lambda Java code:

```bash
cd docker
gradle clean build shadowJar
```

The source code for the CDK application lives in the `infrastructure` directory.
To build and deploy it you will first need to install the dependencies:

```bash
cd ../infrastructure
npm install
```

To deploy the CDK app you will need to have [configured your AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html#cli-chap-authentication-precedence).

You will also need a superuser token generated from the [Momento Console](https://console.gomomento.com).

Then run:

```
export MOMENTO_AUTH_TOKEN=<YOUR_MOMENTO_AUTH_TOKEN>
npm run cdk deploy
```

The lambda does not set up a way to access itself externally, so to run it, you will have to go to `MomentoDockerLambdaJava` in AWS Lambda and run a test.

The lambda is set up to make set and get calls for the key 'key' in the cache 'cache'. You can play around with the code by changing the `lambda/docker/src/main/java/momento/lambda/example/MomentoJavaLambda` file. 