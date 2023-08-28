package momento.lambda.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;

import java.time.Duration;
import java.util.Map;

public class MomentoJavaLambda implements RequestHandler<Map<String, String>, String> {

    private static final String AUTH_TOKEN_ENV_VAR = "MOMENTO_AUTH_TOKEN";
    private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

    private static final String CACHE_NAME = "cache";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    @Override
    public String handleRequest(Map<String, String> event, Context context) {

        final CredentialProvider credentialProvider = new EnvVarCredentialProvider(AUTH_TOKEN_ENV_VAR);

        try (final CacheClient client =
                     CacheClient.create(credentialProvider, Configurations.Lambda.latest(), DEFAULT_ITEM_TTL)) {

            createCache(client, CACHE_NAME);

            System.out.printf("Setting key '%s', value '%s'%n", KEY, VALUE);
            client.set(CACHE_NAME, KEY, VALUE).join();

            System.out.printf("Getting value for key '%s'%n", KEY);

            final GetResponse getResponse = client.get(CACHE_NAME, KEY).join();
            if (getResponse instanceof GetResponse.Hit hit) {
                System.out.printf("Found value for key '%s': '%s'%n", KEY, hit.valueString());
            } else if (getResponse instanceof GetResponse.Miss) {
                System.out.println("Found no value for key " + KEY);
            } else if (getResponse instanceof GetResponse.Error error) {
                System.out.printf(
                        "Unable to look up value for key '%s' with error %s\n", KEY, error.getErrorCode());
                System.out.println(error.getMessage());
            }
        }
        return "Success";
    }

    private static void createCache(CacheClient cacheClient, String cacheName) {
        final CacheCreateResponse createResponse = cacheClient.createCache(cacheName).join();
        if (createResponse instanceof CacheCreateResponse.Success) {
            System.out.println("Cache with name '" + cacheName + "' created.");

        }
        if (createResponse instanceof CacheCreateResponse.Error error) {
            if (error.getCause() instanceof AlreadyExistsException) {
                System.out.println("Cache with name '" + cacheName + "' already exists.");
            } else {
                System.out.println("Unable to create cache with error " + error.getErrorCode());
                System.out.println(error.getMessage());
            }
        }
    }
}