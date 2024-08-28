# PoC example

This example demonstrates most basic usage and can be tested together with serverless [YDB service in Yandex Cloud](https://yandex.cloud/ru/docs/ydb/quickstart).

This example executes queries from [YQL tutorial](https://ydb.tech/docs/en/dev/yql-tutorial).

```bash
go run examples/simple/main.go -a ydb1.serverless.yandexcloud.net:2135 -d "/ru-central1/b1g22gee05t.../etn4nlih...." -y /path/to/your/serviceacc/iam/key.json
```