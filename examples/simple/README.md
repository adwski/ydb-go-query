# PoC example

This example demonstrates most basic usage and can be tested together with serverless [YDB service in Yandex Cloud](https://yandex.cloud/ru/docs/ydb/quickstart).

```bash
go run examples/simple/main.go -a ydb1.serverless.yandexcloud.net:2135 -d "/ru-central1/b1g22gee05t.../etn4nlih...." -y /path/to/your/serviceacc/iam/key.json>
```