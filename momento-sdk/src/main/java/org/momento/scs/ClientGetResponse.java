package org.momento.scs;

import grpc.cache_client.Result;

public class ClientGetResponse<T> extends BaseResponse {
    private T body;
    private Result result;

    public ClientGetResponse(Result result, T body) {
        this.body = body;
        this.result = result;
    }

    public MomentoResult getResult() {
        return this.resultMapper(this.result);
    }

    public T getBody() {
        return body;
    }
}
