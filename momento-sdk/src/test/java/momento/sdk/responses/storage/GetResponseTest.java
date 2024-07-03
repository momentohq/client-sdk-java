package momento.sdk.responses.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Status;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.StoreNotFoundException;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;
import org.junit.jupiter.api.Test;

public class GetResponseTest {
  @Test
  public void testGetResponseFoundWorksOnTheRightType() {
    GetResponse.Found response = GetResponse.Found.of(new byte[] {0, 1, 2, 3});
    assert response.value().getByteArray().get().length == 4;
    assertThrows(ClientSdkException.class, response.value().getString()::orElseThrow);
    assertThrows(ClientSdkException.class, response.value().getLong()::orElseThrow);
    assertThrows(ClientSdkException.class, response.value().getDouble()::orElseThrow);

    response = GetResponse.Found.of("string");
    assertThrows(ClientSdkException.class, response.value().getByteArray()::orElseThrow);
    assert response.value().getString().get().equals("string");
    assertThrows(ClientSdkException.class, response.value().getLong()::orElseThrow);
    assertThrows(ClientSdkException.class, response.value().getDouble()::orElseThrow);

    response = GetResponse.Found.of(42L);
    assertThrows(ClientSdkException.class, response.value().getByteArray()::orElseThrow);
    assertThrows(ClientSdkException.class, response.value().getString()::orElseThrow);
    assert response.value().getLong().get() == 42L;
    assertThrows(ClientSdkException.class, response.value().getDouble()::orElseThrow);

    response = GetResponse.Found.of(3.14);
    assertThrows(ClientSdkException.class, response.value().getByteArray()::orElseThrow);
    assertThrows(ClientSdkException.class, response.value().getString()::orElseThrow);
    assertThrows(ClientSdkException.class, response.value().getLong()::orElseThrow);
    assert response.value().getDouble().get() == 3.14;
  }

  @Test
  public void testConvenienceMethodsOnGetResponse() {
    GetResponse.Found response = GetResponse.Found.of(new byte[] {0, 1, 2, 3});
    assert response.found().isPresent();
    assert response.found().get().value().getByteArray().get().length == 4;

    response = GetResponse.Found.of("string");
    assert response.found().isPresent();
    assert response.found().get().value().getString().get() == "string";

    response = GetResponse.Found.of(42L);
    assert response.found().isPresent();
    assert response.found().get().value().getLong().get() == 42L;

    response = GetResponse.Found.of(3.14);
    assert response.found().isPresent();
    assert response.found().get().value().getDouble().get() == 3.14;

    GetResponse.Error error =
        new GetResponse.Error(
            new StoreNotFoundException(
                new Exception(),
                new MomentoTransportErrorDetails(
                    new MomentoGrpcErrorDetails(Status.Code.NOT_FOUND, "not found"))));
    assert error.found().isEmpty();
    assertThrows(ClientSdkException.class, error.found()::orElseThrow);
  }
}
