package momento.sdk;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import grpc.permission_messages.CacheRole;
import grpc.permission_messages.TopicRole;
import grpc.permission_messages.ExplicitPermissions;
import grpc.permission_messages.Permissions;
import grpc.permission_messages.PermissionsType;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.accessControl.CacheItemSelector;
import momento.sdk.auth.accessControl.CacheSelector;
import momento.sdk.auth.accessControl.DisposableToken;
import momento.sdk.auth.accessControl.DisposableTokenPermission;
import momento.sdk.auth.accessControl.DisposableTokenScope;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.auth.accessControl.TopicSelector;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.UnknownException;
import momento.sdk.responses.GenerateDisposableTokenResponse;
import momento.token._GenerateDisposableTokenRequest;
import momento.token._GenerateDisposableTokenResponse;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public class TokenClient {
    private final CredentialProvider credentialProvider;
    private final TokenGrpcStubsManager tokenGrpcStubsManager;


    public TokenClient(@Nonnull CredentialProvider credentialProvider) {
        this.credentialProvider = credentialProvider;
        this.tokenGrpcStubsManager = new TokenGrpcStubsManager(credentialProvider);
    }

    private Permissions permissionsFromDisposableTokenScope(DisposableTokenScope scope) {
        Permissions.Builder permissions = Permissions.newBuilder();
        ExplicitPermissions.Builder explicitPermissions = ExplicitPermissions.newBuilder();

        for (DisposableTokenPermission perm : scope.getPermissions()) {
            PermissionsType grpcPerm = disposableTokenPermissionToGrpcPermission(perm);
            explicitPermissions.addPermissions(grpcPerm);
        }

        permissions.setExplicit(explicitPermissions.build());
        return permissions.build();
    }

    private PermissionsType disposableTokenPermissionToGrpcPermission(DisposableTokenPermission permission) {
        PermissionsType.Builder result = PermissionsType.newBuilder();
        if (permission instanceof DisposableToken.CachePermission) {
            PermissionsType.CachePermissions.Builder cachePerms = PermissionsType.CachePermissions.newBuilder();
            switch (((DisposableToken.CachePermission) permission).getRole()) {
                case ReadWrite:
                    cachePerms.setRole(CacheRole.CacheReadWrite);
                    break;
                case ReadOnly:
                    cachePerms.setRole(CacheRole.CacheReadOnly);
                    break;
                case WriteOnly:
                    cachePerms.setRole(CacheRole.CacheWriteOnly);
                    break;
            }
            CacheSelector cacheSelector = ((DisposableToken.CachePermission) permission).getCacheSelector();
            if (cacheSelector instanceof CacheSelector.SelectAllCaches) {
                cachePerms.setAllCaches(PermissionsType.All.newBuilder().build());
            } else if (cacheSelector instanceof CacheSelector.SelectByCacheName) {
                CacheSelector.SelectByCacheName byCacheName = (CacheSelector.SelectByCacheName) cacheSelector;
                cachePerms.setCacheSelector(PermissionsType.CacheSelector.newBuilder().setCacheName(byCacheName.CacheName).build());
            }

            CacheItemSelector cacheItemSelector = ((DisposableToken.CacheItemPermission) permission).getCacheItemSelector();
            if (cacheItemSelector instanceof CacheItemSelector.SelectAllCacheItems) {
                cachePerms.setAllItems(PermissionsType.All.newBuilder().build());
            } else if (cacheItemSelector instanceof CacheItemSelector.SelectByKey) {
                CacheItemSelector.SelectByKey byKey = (CacheItemSelector.SelectByKey) cacheItemSelector;
                cachePerms.setItemSelector(PermissionsType.CacheItemSelector.newBuilder().setKey(ByteString.copyFromUtf8(byKey.CacheKey)).build());
            } else if (cacheItemSelector instanceof CacheItemSelector.SelectByKeyPrefix ) {
                CacheItemSelector.SelectByKeyPrefix byKeyPrefix = (CacheItemSelector.SelectByKeyPrefix) cacheItemSelector;
                cachePerms.setItemSelector(PermissionsType.CacheItemSelector.newBuilder().setKeyPrefix(ByteString.copyFromUtf8(byKeyPrefix.CacheKeyPrefix)).build());
            } else {
                Gson gson = new Gson();
                throw new UnknownException(
                        "Unrecognized cache item specification in cache permission: " +
                                gson.toJson(permission)
                );
            }

            result.setCachePermissions(cachePerms.build());

        } else if (permission instanceof DisposableToken.TopicPermission) {
            PermissionsType.TopicPermissions.Builder topicPerms = PermissionsType.TopicPermissions.newBuilder();
            switch (((DisposableToken.TopicPermission) permission).getRole()) {
                case PublishOnly:
                    topicPerms.setRole(TopicRole.TopicWriteOnly);
                    break;
                case SubscribeOnly:
                    topicPerms.setRole(TopicRole.TopicReadOnly);
                    break;
                case PublishSubscribe:
                    topicPerms.setRole(TopicRole.TopicReadWrite);
                    break;
            }

            CacheSelector cacheSelector = ((DisposableToken.TopicPermission) permission).getCacheSelector();
            if (cacheSelector instanceof CacheSelector.SelectAllCaches) {
                topicPerms.setAllCaches(PermissionsType.All.newBuilder().build());
            } else if (cacheSelector instanceof CacheSelector.SelectByCacheName) {
                CacheSelector.SelectByCacheName byCacheName = (CacheSelector.SelectByCacheName) cacheSelector;
                topicPerms.setCacheSelector(PermissionsType.CacheSelector.newBuilder().setCacheName(byCacheName.CacheName).build());
            }

            TopicSelector topicSelector = ((DisposableToken.TopicPermission) permission).getTopicSelector();
            if (topicSelector instanceof  TopicSelector.SelectAllTopics) {
                topicPerms.setAllTopics(PermissionsType.All.newBuilder().build());
            } else if (topicSelector instanceof TopicSelector.SelectByTopicName) {
                TopicSelector.SelectByTopicName byTopicName = (TopicSelector.SelectByTopicName) topicSelector;
                topicPerms.setTopicSelector(PermissionsType.TopicSelector.newBuilder().setTopicName(byTopicName.TopicName).build());
            } else if (topicSelector instanceof TopicSelector.SelectByTopicNamePrefix) {
                TopicSelector.SelectByTopicNamePrefix byTopicNamePrefix = (TopicSelector.SelectByTopicNamePrefix) topicSelector;
                topicPerms.setTopicSelector(PermissionsType.TopicSelector.newBuilder().setTopicNamePrefix(byTopicNamePrefix.TopicNamePrefix).build());
            }

            result.setTopicPermissions(topicPerms.build());
        }

        return result.build();
    }

    CompletableFuture<GenerateDisposableTokenResponse> generateDisposableToken(DisposableTokenScope scope, ExpiresIn expiresIn, String tokenId) {
        Permissions permissions;
        try {
            permissions = permissionsFromDisposableTokenScope(scope);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new GenerateDisposableTokenResponse.Error(
                    new InvalidArgumentException(e.getMessage())
            ));
        }

        try {
            _GenerateDisposableTokenRequest request = _GenerateDisposableTokenRequest.newBuilder()
                    .setPermissions(permissions)
                    .setTokenId(tokenId)
                    .setAuthToken(credentialProvider.getAuthToken())
                    .setExpires(_GenerateDisposableTokenRequest.Expires.newBuilder().setValidForSeconds(expiresIn.getSeconds()).build())
                    .build();
            _GenerateDisposableTokenResponse response = tokenGrpcStubsManager.getStub().generateDisposableToken(request).get();
            return CompletableFuture.completedFuture(new GenerateDisposableTokenResponse.Success(response));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new GenerateDisposableTokenResponse.Error(
                    new InvalidArgumentException(e.getMessage())
            ));
        }
    }

    CompletableFuture<GenerateDisposableTokenResponse> generateDisposableToken(DisposableTokenScope scope, ExpiresIn expiresIn) {
        Permissions permissions;
        try {
            permissions = permissionsFromDisposableTokenScope(scope);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new GenerateDisposableTokenResponse.Error(
                    new InvalidArgumentException(e.getMessage())
            ));
        }

        try {
            _GenerateDisposableTokenRequest request = _GenerateDisposableTokenRequest.newBuilder()
                    .setPermissions(permissions)
                    .setTokenId("")
                    .setAuthToken(credentialProvider.getAuthToken())
                    .setExpires(_GenerateDisposableTokenRequest.Expires.newBuilder().setValidForSeconds(expiresIn.getSeconds()).build())
                    .build();
            _GenerateDisposableTokenResponse response = tokenGrpcStubsManager.getStub().generateDisposableToken(request).get();
            return CompletableFuture.completedFuture(new GenerateDisposableTokenResponse.Success(response));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new GenerateDisposableTokenResponse.Error(
                    new InvalidArgumentException(e.getMessage())
            ));
        }
    }

    public void close()
    {
        this.tokenGrpcStubsManager.close();
    }
}
