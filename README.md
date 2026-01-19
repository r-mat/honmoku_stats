# honmoku_stats
This is a Python projects which get statistics information of Honmoku Umizuri Shisetsu in Yokohama.

## カスタムドメインの設定

API Gatewayにカスタムドメインを設定する場合、以下の手順を実行してください。

**注意**: ドメイン、ACM証明書、Route53レコードは手動で作成してください。テンプレートでは自動作成しません。

### 1. ACM証明書の作成

#### オプションA: AWSコンソールで作成
1. AWSコンソールでACM（Certificate Manager）を開く
2. 「証明書をリクエスト」をクリック
3. ドメイン名を入力（例: `api.example.com`）
4. DNS検証を選択
5. Route53でホストゾーンを管理している場合、検証レコードを自動的に作成
6. 証明書が発行されるまで待つ（通常数分〜数時間）
7. 発行された証明書のARNをメモしておく

#### オプションB: AWS CLIで作成
```bash
aws acm request-certificate \
  --domain-name api.example.com \
  --validation-method DNS
```

### 2. Route53レコードの作成

API Gatewayのカスタムドメインを作成した後、Route53でAレコードを作成してください。

1. デプロイ後に`ApiCustomDomain`リソースの`RegionalDomainName`と`RegionalHostedZoneId`を確認
2. Route53でホストゾーンを開く
3. レコードを作成：
   - レコード名: `api.example.com`（サブドメイン名）
   - レコードタイプ: A
   - エイリアス: はい
   - エイリアス先: API Gatewayのカスタムドメイン
   - リージョン: デプロイしたリージョン
   - エイリアスホストゾーンID: `RegionalHostedZoneId`の値

### 3. デプロイ時のパラメータ指定

`sam deploy`実行時に以下のパラメータを指定：

```bash
sam deploy \
  --parameter-overrides \
    ApiDomainName=api.example.com \
    HostedZoneId=Z1234567890ABC \
    CertificateArn=arn:aws:acm:REGION:123456789012:certificate/12345678-1234-1234-1234-123456789012 \
    AppSyncApiKey=your-api-key \
    SesFrom=from@example.com \
    SesTo=to@example.com
```

### 4. パラメータの説明

- `ApiDomainName`: カスタムドメイン名（例: `api.example.com`）
- `HostedZoneId`: Route53のホストゾーンID（例: `Z1234567890ABC`）- 参考情報として使用（Route53レコードは手動作成）
- `CertificateArn`: 手動で作成したACM証明書のARN（必須）
- `AppSyncApiKey`: AppSync APIの認証キー
- `SesFrom`: SESの送信元メールアドレス
- `SesTo`: SESの送信先メールアドレス

### 5. カスタムドメインを使用しない場合

カスタムドメインを使用しない場合は、`ApiDomainName`を空文字列（または指定しない）にしてください。デフォルトのAPI Gatewayエンドポイントが使用されます。

```bash
sam deploy \
  --parameter-overrides \
    AppSyncApiKey=your-api-key \
    SesFrom=from@example.com \
    SesTo=to@example.com
```
