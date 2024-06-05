package momento.sdk.responses.storage;

import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.responses.storage.data.GetResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class GetResponseTest {
    @Test
    public void testGetResponseSuccessWorksOnTheRightType() {
        GetResponse.Success response = GetResponse.Success.of(new byte[] { 0, 1, 2, 3 });
        assert response.getValueAsByteArray().length == 4;

        response = GetResponse.Success.of("string");
        assert response.getValueAsString().equals("string");

        response = GetResponse.Success.of(42L);
        assert response.getValueAsLong() == 42L;

        response = GetResponse.Success.of(3.14);
        assert response.getValueAsDouble() == 3.14;
    }

    @Test
    public void testGetResponseSuccessThrowsExceptionOnWrongType() {
        GetResponse.Success response = GetResponse.Success.of(new byte[] { 0, 1, 2, 3 });
        assertThrows(ClientSdkException.class, response::getValueAsString);

        response = GetResponse.Success.of("string");
        assertThrows(ClientSdkException.class, response::getValueAsLong);

        response = GetResponse.Success.of(42L);
        assertThrows(ClientSdkException.class, response::getValueAsDouble);

        response = GetResponse.Success.of(3.14);
        assertThrows(ClientSdkException.class, response::getValueAsByteArray);
    }
}
