package momento.sdk.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MomentoLocalProviderTest {

  @Test
  void testDefaultConstructor() {
    MomentoLocalProvider provider = new MomentoLocalProvider();

    assertEquals("127.0.0.1", provider.getCacheEndpoint());
    assertEquals("127.0.0.1", provider.getControlEndpoint());
    assertEquals("127.0.0.1", provider.getTokenEndpoint());
    assertEquals("127.0.0.1", provider.getStorageEndpoint());
    assertEquals(8080, provider.getPort());
    assertFalse(provider.isCacheEndpointSecure());
    assertFalse(provider.isControlEndpointSecure());
    assertFalse(provider.isTokenEndpointSecure());
    assertFalse(provider.isStorageEndpointSecure());
    assertEquals("", provider.getAuthToken());
  }

  @Test
  void testConstructorWithHostnameAndPort() {
    MomentoLocalProvider provider = new MomentoLocalProvider("localhost", 9090);

    assertEquals("localhost", provider.getCacheEndpoint());
    assertEquals("localhost", provider.getControlEndpoint());
    assertEquals("localhost", provider.getTokenEndpoint());
    assertEquals("localhost", provider.getStorageEndpoint());
    assertEquals(9090, provider.getPort());
    assertFalse(provider.isCacheEndpointSecure());
  }

  @Test
  void testConstructorWithHostnameOnly() {
    MomentoLocalProvider provider = new MomentoLocalProvider("custom-host");

    assertEquals("custom-host", provider.getCacheEndpoint());
    assertEquals("custom-host", provider.getControlEndpoint());
    assertEquals("custom-host", provider.getTokenEndpoint());
    assertEquals("custom-host", provider.getStorageEndpoint());
    assertEquals(8080, provider.getPort());
  }

  @Test
  void testConstructorWithPortOnly() {
    MomentoLocalProvider provider = new MomentoLocalProvider(7070);

    assertEquals("127.0.0.1", provider.getCacheEndpoint());
    assertEquals("127.0.0.1", provider.getControlEndpoint());
    assertEquals("127.0.0.1", provider.getTokenEndpoint());
    assertEquals("127.0.0.1", provider.getStorageEndpoint());
    assertEquals(7070, provider.getPort());
  }

  @Test
  void testSecureEndpointDetection() {
    MomentoLocalProvider provider = new MomentoLocalProvider("https://secure-host");

    assertTrue(provider.isCacheEndpointSecure());
    assertTrue(provider.isControlEndpointSecure());
    assertTrue(provider.isTokenEndpointSecure());
    assertTrue(provider.isStorageEndpointSecure());
  }

  @Test
  void testInsecureEndpointDetection() {
    MomentoLocalProvider provider = new MomentoLocalProvider("http://insecure-host");

    assertFalse(provider.isCacheEndpointSecure());
    assertFalse(provider.isControlEndpointSecure());
    assertFalse(provider.isTokenEndpointSecure());
    assertFalse(provider.isStorageEndpointSecure());
  }
}
