# Java クライアント SDK

_他言語もあります_: [English](README.md)

<br>

## SDK コード例を実行する

- Gradle のインストールは必要ありません。
- このコード例を実行するには JDK 11 もしくはそれ以上が必要です。
- Momento オーストークンが必要です。トークン発行は[Momento CLI](https://github.com/momentohq/momento-cli)から行えます。

```bash
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> ./gradlew run
```

SDK コード例: [MomentoApplication.java](lib/src/main/java/momento/client/example/MomentoCacheApplication.java)

## Java SDK を自身のプロジェクトで使用する

### Gradle コンフィグレーション

コンポネントをプロジェクトに追加するには Gradle ビルドをアップデートしてください。

**build.gradle.kts**

```kotlin
repositories {
    maven("https://momento.jfrog.io/artifactory/maven-public")
}

dependencies {
    implementation("momento.sandbox:momento-sdk:0.18.0")
}
```
