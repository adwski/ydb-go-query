# PoC example

This example showcases main features using queries from [YQL tutorial](https://ydb.tech/docs/en/dev/yql-tutorial).

Run it together with serverless [YDB service in Yandex Cloud](https://yandex.cloud/ru/docs/ydb/quickstart):

```bash
go run examples/showcase/*.go \
  -a ydb.serverless.yandexcloud.net:2135 \
  -d "/ru-central1/b1g22gee05t.../etn4nlih...." \
  -y /path/to/your/serviceacc/iam/key.json
```
