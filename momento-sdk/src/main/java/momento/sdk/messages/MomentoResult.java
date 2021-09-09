package momento.sdk.messages;
import grpc.cache_client.Result;

public enum MomentoResult {
    Internal_Server_Error(Result.Internal_Server_Error),
    Ok(Result.Ok),
    Hit(Result.Hit),
    Miss(Result.Miss),
    Bad_Request(Result.Bad_Request),
    Unauthorized(Result.Unauthorized),
    Service_Unavailable(Result.Service_Unavailable),
    Unknown(65535);

    private int result;

    MomentoResult(grpc.cache_client.Result num) {
        this.result = num.getNumber();
    }

    MomentoResult(int num) {
        this.result = num;
    }

    public int getResult() {
        return this.result;
    }
}
