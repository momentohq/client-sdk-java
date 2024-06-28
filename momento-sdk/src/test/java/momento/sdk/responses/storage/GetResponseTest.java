package momento.sdk.responses.storage;

import io.grpc.Status;
import momento.sdk.exceptions.StoreNotFoundException;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;
import org.junit.jupiter.api.Test;

public class GetResponseTest {
  @Test
  public void testGetResponseFoundWorksOnTheRightType() {
    GetResponse.Found response = GetResponse.Found.of(new byte[] {0, 1, 2, 3});
    assert response.value().getByteArray().get().length == 4;
    assert !response.value().getString().isPresent();
    assert !response.value().getLong().isPresent();
    assert !response.value().getDouble().isPresent();

    response = GetResponse.Found.of("string");
    assert !response.value().getByteArray().isPresent();
    assert response.value().getString().get().equals("string");
    assert !response.value().getLong().isPresent();
    assert !response.value().getDouble().isPresent();

    response = GetResponse.Found.of(42L);
    assert !response.value().getByteArray().isPresent();
    assert !response.value().getString().isPresent();
    assert response.value().getLong().get() == 42L;
    assert !response.value().getDouble().isPresent();

    response = GetResponse.Found.of(3.14);
    assert !response.value().getByteArray().isPresent();
    assert !response.value().getString().isPresent();
    assert !response.value().getLong().isPresent();
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
    assert !error.found().isPresent();
  }
}
