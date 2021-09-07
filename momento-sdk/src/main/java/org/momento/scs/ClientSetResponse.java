package org.momento.scs;

import grpc.cache_client.Result;

public class ClientSetResponse extends BaseResponse {
    private Result result;

    public ClientSetResponse(Result result) {
        this.result = result;
    }

    public MomentoResult getResult() {
        return this.resultMapper(this.result);
    }
}
