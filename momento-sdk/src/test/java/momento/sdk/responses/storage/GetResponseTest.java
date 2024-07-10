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
    final GetResponse.Found response = GetResponse.Found.of(new byte[] {0, 1, 2, 3});
    assert response.value().getByteArray().get().length == 4;
    assertThrows(ClientSdkException.class, response.value().getString()::get);
    assertThrows(
        RuntimeException.class,
        () -> response.value().getString().orElseThrow(() -> new RuntimeException("derp")));
    assertThrows(ClientSdkException.class, response.value().getLong()::get);
    assertThrows(
        RuntimeException.class,
        () -> response.value().getLong().orElseThrow(() -> new RuntimeException("derp")));
    assertThrows(ClientSdkException.class, response.value().getDouble()::get);
    assertThrows(
        RuntimeException.class,
        () -> response.value().getDouble().orElseThrow(() -> new RuntimeException("derp")));

    final GetResponse.Found response2 = GetResponse.Found.of("string");
    assertThrows(ClientSdkException.class, response2.value().getByteArray()::get);
    assertThrows(
        RuntimeException.class,
        () -> response2.value().getByteArray().orElseThrow(() -> new RuntimeException("derp")));
    assert response2.value().getString().get().equals("string");
    assertThrows(ClientSdkException.class, response2.value().getLong()::get);
    assertThrows(
        RuntimeException.class,
        () -> response2.value().getLong().orElseThrow(() -> new RuntimeException("derp")));
    assertThrows(ClientSdkException.class, response2.value().getDouble()::get);
    assertThrows(
        RuntimeException.class,
        () -> response2.value().getDouble().orElseThrow(() -> new RuntimeException("derp")));

    final GetResponse.Found response3 = GetResponse.Found.of(42L);
    assertThrows(ClientSdkException.class, response3.value().getByteArray()::get);
    assertThrows(
        RuntimeException.class,
        () -> response3.value().getByteArray().orElseThrow(() -> new RuntimeException("derp")));
    assertThrows(ClientSdkException.class, response3.value().getString()::get);
    assertThrows(
        RuntimeException.class,
        () -> response3.value().getString().orElseThrow(() -> new RuntimeException("derp")));
    assert response3.value().getLong().get() == 42L;
    assertThrows(ClientSdkException.class, response3.value().getDouble()::get);
    assertThrows(
        RuntimeException.class,
        () -> response3.value().getDouble().orElseThrow(() -> new RuntimeException("derp")));

    final GetResponse.Found response4 = GetResponse.Found.of(3.14);
    assertThrows(ClientSdkException.class, response4.value().getByteArray()::get);
    assertThrows(
        RuntimeException.class,
        () -> response4.value().getByteArray().orElseThrow(() -> new RuntimeException("derp")));
    assertThrows(ClientSdkException.class, response4.value().getString()::get);
    assertThrows(
        RuntimeException.class,
        () -> response4.value().getString().orElseThrow(() -> new RuntimeException("derp")));
    assertThrows(ClientSdkException.class, response4.value().getLong()::get);
    assertThrows(
        RuntimeException.class,
        () -> response4.value().getLong().orElseThrow(() -> new RuntimeException("derp")));
    assert response4.value().getDouble().get() == 3.14;
  }

  @Test
  public void testConvenienceMethodsOnGetResponse() {
    GetResponse.Found response = GetResponse.Found.of(new byte[] {0, 1, 2, 3});
    assert response.valueWhenFound().isPresent();
    assert response.valueWhenFound().get().getByteArray().get().length == 4;

    response = GetResponse.Found.of("string");
    assert response.valueWhenFound().isPresent();
    assert response.valueWhenFound().get().getString().get() == "string";

    response = GetResponse.Found.of(42L);
    assert response.valueWhenFound().isPresent();
    assert response.valueWhenFound().get().getLong().get() == 42L;

    response = GetResponse.Found.of(3.14);
    assert response.valueWhenFound().isPresent();
    assert response.valueWhenFound().get().getDouble().get() == 3.14;

    GetResponse.Error error =
        new GetResponse.Error(
            new StoreNotFoundException(
                new Exception(),
                new MomentoTransportErrorDetails(
                    new MomentoGrpcErrorDetails(Status.Code.NOT_FOUND, "not found"))));
    assert error.valueWhenFound().isEmpty();
    assertThrows(ClientSdkException.class, error.valueWhenFound()::get);
    assertThrows(
        RuntimeException.class,
        () -> error.valueWhenFound().orElseThrow(() -> new RuntimeException("derp")));
  }
}
