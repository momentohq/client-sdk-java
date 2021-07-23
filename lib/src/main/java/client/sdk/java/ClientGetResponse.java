package client.sdk.java;

import grpc.cache_client.Result;

public class ClientGetResponse<T> {
    private T body;
    private Result result;

    ClientGetResponse(Result result, T body) {
        this.body = body;
        this.result = result;
    }

    public Result getResult() {
        return result;
    }

    public T getBody() {
        return body;
    }
}
