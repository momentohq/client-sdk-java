package momento.sdk.responses.storage;

import io.grpc.Status;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;
import org.junit.jupiter.api.Test;

public class GetResponseTest {
  @Test
  public void testGetResponseSuccessWorksOnTheRightType() {
    GetResponse.Success response = GetResponse.Success.of(new byte[] {0, 1, 2, 3});
    assert response.value().get().getByteArray().get().length == 4;
    assert !response.value().get().getString().isPresent();
    assert !response.value().get().getLong().isPresent();
    assert !response.value().get().getDouble().isPresent();

    response = GetResponse.Success.of("string");
    assert !response.value().get().getByteArray().isPresent();
    assert response.value().get().getString().get().equals("string");
    assert !response.value().get().getLong().isPresent();
    assert !response.value().get().getDouble().isPresent();

    response = GetResponse.Success.of(42L);
    assert !response.value().get().getByteArray().isPresent();
    assert !response.value().get().getString().isPresent();
    assert response.value().get().getLong().get() == 42L;
    assert !response.value().get().getDouble().isPresent();

    response = GetResponse.Success.of(3.14);
    assert !response.value().get().getByteArray().isPresent();
    assert !response.value().get().getString().isPresent();
    assert !response.value().get().getLong().isPresent();
    assert response.value().get().getDouble().get() == 3.14;
  }

  @Test
  public void testConvenienceMethodsOnGetResponse() {
    GetResponse.Success response = GetResponse.Success.of(new byte[] {0, 1, 2, 3});
    assert response.success().isPresent();
    assert response.success().get().value().get().getByteArray().get().length == 4;

    response = GetResponse.Success.of("string");
    assert response.success().isPresent();
    assert response.success().get().value().get().getString().get() == "string";

    response = GetResponse.Success.of(42L);
    assert response.success().isPresent();
    assert response.success().get().value().get().getLong().get() == 42L;

    response = GetResponse.Success.of(3.14);
    assert response.success().isPresent();
    assert response.success().get().value().get().getDouble().get() == 3.14;

    // TODO distinguish store not found from key not found
    GetResponse.Error error =
        new GetResponse.Error(
            new NotFoundException(
                new Exception(),
                new MomentoTransportErrorDetails(
                    new MomentoGrpcErrorDetails(Status.Code.NOT_FOUND, "not found"))));
    assert !error.success().isPresent();
  }
}
