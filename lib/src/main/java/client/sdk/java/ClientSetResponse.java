package client.sdk.java;

import grpc.cache_client.Result;

public class ClientSetResponse {
    private Result result;

    ClientSetResponse(Result result) {
        this.result = result;
    }

    public Result getResult() {
        return result;
    }
}
