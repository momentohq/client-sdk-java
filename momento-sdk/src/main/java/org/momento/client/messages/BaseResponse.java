package org.momento.client.messages;

import grpc.cache_client.Result;

// TODO: This should be made package default
public class BaseResponse {
    MomentoResult resultMapper(Result result) {
        switch (result) {
            case Ok: return MomentoResult.Ok;
            case Hit: return MomentoResult.Hit;
            case Miss: return MomentoResult.Miss;
            case Unauthorized: return MomentoResult.Unauthorized;
            case Bad_Request: return MomentoResult.Bad_Request;
            case Service_Unavailable: return MomentoResult.Service_Unavailable;
            case Internal_Server_Error: return MomentoResult.Internal_Server_Error;
            default: return MomentoResult.Unknown;
        }
    }
}
