package momento.sdk.responses.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import momento.sdk.exceptions.ClientSdkException;
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
}
