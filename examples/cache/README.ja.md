# Java クライアント SDK

_他言語もあります_: [English](README.md)

<br>

## SDK コード例を実行する

- Gradle のインストールは必要ありません。
- このコード例を実行するには JDK 14 もしくはそれ以上が必要です。
- Momento オーストークンが必要です。トークン発行は[Momento CLI](https://github.com/momentohq/momento-cli)から行えます。

```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew basic
```

SDK コード例: [MomentoApplication.java](src/main/java/momento/client/example/BasicExample.java)

## Java SDK を自身のプロジェクトで使用する

### Gradle コンフィグレーション

コンポネントをプロジェクトに追加するには Gradle ビルドをアップデートしてください。

**build.gradle.kts**

```kotlin
dependencies {
    implementation("software.momento.java:sdk:0.24.0")
}
```
