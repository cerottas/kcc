# Artifact Gateway v1 契约

KCC 只依赖这个窄 HTTP 边界，Material/Artifact Keeper 的内部模型可以独立演进。
生产环境应使用 TLS；Bearer token 由 Kubernetes Secret 投影，不能写进 CR。

## 引用

URI 固定为 `artifact://<namespace>/<name>/<version>`。三个字段非空且不包含空白；
version 必须不可变。输入 source/model/data 与训练输出使用同一格式。

## 解析/下载

`GET /v1/artifacts/{namespace}/{name}/versions/{version}` 返回：

```json
{
  "downloadUrl": "https://object-store/...",
  "sha256": "64位小写十六进制",
  "size": 123,
  "mediaType": "application/vnd.kcc.directory+tar.gz"
}
```

`downloadUrl` 必须是无需转发 KCC Bearer token 的短期预签名 URL，或仅在集群内可读的
HTTP(S) URL。KCC 有意不把 Gateway token 发送给任意对象存储 host。KCC 在解包前校验
长度和 SHA256，并拒绝绝对路径、`..`、重复路径、软/硬链接、设备和 FIFO；物化目录带
内容标记，不能被另一个 digest 覆盖。

## 发布输出

`PUT /v1/artifacts/{namespace}/{name}/versions/{version}`，请求头：

- `Content-Type: application/vnd.kcc.directory+tar.gz`
- `Content-Length: <bytes>`
- `X-Content-SHA256: <sha256>`
- `If-None-Match: *`
- `Authorization: Bearer ...`（启用认证时）

响应必须是 200/201，且正文精确确认：

```json
{"uri":"artifact://ns/name/version","sha256":"...","size":123}
```

同一 URI + 同一 digest 的重放必须幂等地返回相同收据；同一 URI + 不同 digest 必须
返回冲突且不得覆盖。Gateway 在返回成功前必须完成对象持久化和 manifest 提交。

输出名为 `<TrainingRun.name>-output`，version 为
`attempt-NN-<archive-sha256前16位>`。控制器会校验 namespace、name、attempt，拒绝伪造
或错配的成功结果。
