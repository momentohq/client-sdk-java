package momento.sdk.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class MomentoLocalProviderTest {

  @Test
  void testDefaultConstructor() {
    String hostname = "127.0.0.1";
    MomentoLocalProvider provider = new MomentoLocalProvider();

    assertEquals(hostname, provider.getCacheEndpoint());
    assertEquals(hostname, provider.getControlEndpoint());
    assertEquals(hostname, provider.getTokenEndpoint());
    assertEquals(hostname, provider.getStorageEndpoint());
    assertEquals(8080, provider.getPort());
    assertFalse(provider.isEndpointSecure());
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
    assertFalse(provider.isEndpointSecure());
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
  void testInsecureEndpointDetection() {
    String insecureHost = "http://insecure-host";
    MomentoLocalProvider provider = new MomentoLocalProvider(insecureHost);
    assertFalse(provider.isEndpointSecure());
  }
}
