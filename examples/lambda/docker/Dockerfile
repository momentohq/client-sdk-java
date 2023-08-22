FROM --platform=linux/amd64 public.ecr.aws/lambda/java:17

# Copy the build jar file to the root of the image
COPY build/libs/docker-1.0-SNAPSHOT-all.jar ${LAMBDA_TASK_ROOT}/lib/

# Set the CMD to your handler (could be a different class path for your handler)
CMD ["momento.lambda.example.MomentoJavaLambda::handleRequest"]
