package momento.sdk.responses.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Status;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;
import momento.sdk.responses.storage.data.GetResponse;
import org.junit.jupiter.api.Test;

public class GetResponseTest {
  @Test
  public void testGetResponseSuccessWorksOnTheRightType() {
    GetResponse.Success response = GetResponse.Success.of(new byte[] {0, 1, 2, 3});
    assert response.valueByteArray().length == 4;

    response = GetResponse.Success.of("string");
    assert response.valueString().equals("string");

    response = GetResponse.Success.of(42L);
    assert response.valueLong() == 42L;

    response = GetResponse.Success.of(3.14);
    assert response.valueDouble() == 3.14;
  }

  @Test
  public void testGetResponseSuccessThrowsExceptionOnWrongType() {
    GetResponse.Success response = GetResponse.Success.of(new byte[] {0, 1, 2, 3});
    assertThrows(ClientSdkException.class, response::valueString);

    response = GetResponse.Success.of("string");
    assertThrows(ClientSdkException.class, response::valueLong);

    response = GetResponse.Success.of(42L);
    assertThrows(ClientSdkException.class, response::valueDouble);

    response = GetResponse.Success.of(3.14);
    assertThrows(ClientSdkException.class, response::valueByteArray);
  }

  @Test
  public void testConvenienceMethodsOnGetResponse() {
    GetResponse.Success response = GetResponse.Success.of(new byte[] {0, 1, 2, 3});
    assert response.tryValueByteArray().isPresent();
    assertThrows(ClientSdkException.class, response::tryValueString);
    assertThrows(ClientSdkException.class, response::tryValueLong);
    assertThrows(ClientSdkException.class, response::tryValueDouble);

    response = GetResponse.Success.of("string");
    assertThrows(ClientSdkException.class, response::tryValueByteArray);
    assert response.tryValueString().isPresent();
    assertThrows(ClientSdkException.class, response::tryValueLong);
    assertThrows(ClientSdkException.class, response::tryValueDouble);

    response = GetResponse.Success.of(42L);
    assertThrows(ClientSdkException.class, response::tryValueByteArray);
    assertThrows(ClientSdkException.class, response::tryValueString);
    assert response.tryValueLong().isPresent();
    assertThrows(ClientSdkException.class, response::tryValueDouble);

    response = GetResponse.Success.of(3.14);
    assertThrows(ClientSdkException.class, response::tryValueByteArray);
    assertThrows(ClientSdkException.class, response::tryValueString);
    assertThrows(ClientSdkException.class, response::tryValueLong);
    assert response.tryValueDouble().isPresent();

    // TODO distinguish store not found from key not found
    GetResponse.Error error =
        new GetResponse.Error(
            new NotFoundException(
                new Exception(),
                new MomentoTransportErrorDetails(
                    new MomentoGrpcErrorDetails(Status.Code.NOT_FOUND, "not found"))));
    assert !error.tryValueDouble().isPresent();
    assert !error.tryValueLong().isPresent();
    assert !error.tryValueString().isPresent();
    assert !error.tryValueByteArray().isPresent();
  }
}
