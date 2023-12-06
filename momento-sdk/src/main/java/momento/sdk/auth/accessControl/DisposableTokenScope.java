package momento.sdk.auth.accessControl;

import java.util.List;

public class DisposableTokenScope {
  private final List<DisposableTokenPermission> permissions;

  public DisposableTokenScope(List<DisposableTokenPermission> permissions) {
    this.permissions = permissions;
  }

  public List<DisposableTokenPermission> getPermissions() {
    return permissions;
  }
}
