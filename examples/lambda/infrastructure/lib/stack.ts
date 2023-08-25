import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';

export class MomentoLambdaStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        if (!process.env.MOMENTO_AUTH_TOKEN) {
            throw new Error('The environment variable MOMENTO_AUTH_TOKEN must be set.');
        }

        // Create Lambda function from Docker Image
        const dockerLambda = new lambda.DockerImageFunction(this, 'MomentoDockerLambdaJava', {
            functionName: 'MomentoDockerLambdaJava',
            code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '../../docker')), // Point to the root since Dockerfile should be there
            environment: {
                MOMENTO_AUTH_TOKEN: process.env.MOMENTO_AUTH_TOKEN || ''
            },
            memorySize: 128,
            timeout: cdk.Duration.seconds(30)
        });
    }
}
