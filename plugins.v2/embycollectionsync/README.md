# Emby榜单合集同步

基于 `ffinly/emby-collection-sync` 移植的 MoviePilot V2 插件。

## 功能

- 同步 TMDb List 到 Emby BoxSet 合集。
- 内置朋友项目维护的 10 个核心榜单与 84 个豆瓣分类榜单，可在插件配置页勾选执行。
- 支持自定义 TMDb 合集榜单 JSON，用户可按样例自行填写。
- 自动写入榜单简介、注入 TMDb 原语言海报。
- 支持国产电影/电视剧自动合集。
- 支持扫描并修复无封面的普通合集。
- 可将榜单缺失项直接加入 MoviePilot 订阅，`mp_subscribe` 支持 `true`、`false`、数字排名阈值。
- 支持电影/剧集 TMDb ID 排除列表，避免自动订阅指定条目。

## 配置

安装后在插件配置页填写：

- Emby 地址与 API Key。
- TMDb v3 API Key。
- 是否启用代理。
- 执行周期，默认 `30 8 * * *`。
- 内置核心榜单与内置豆瓣分类榜单多选框。
- 自定义合集榜单 JSON。

缺失项订阅使用 MoviePilot 内部 `SubscribeChain`，不再需要单独填写 MoviePilot URL/API Token。

## 自定义榜单 JSON 样例

```json
[
  {
    "name": "示例 - TMDb 自定义电影榜",
    "id": "123456",
    "type": "Movie",
    "mp_subscribe": false
  },
  {
    "name": "示例 - 只订阅前 3 名",
    "id": "654321",
    "type": "Movie",
    "mp_subscribe": 3
  }
]
```

字段说明：

- `name`: Emby 合集名称。
- `id`: TMDb List ID。
- `type`: `Movie` 或 `Series`。
- `mp_subscribe`: `true` 表示缺失即订阅，`false` 表示仅报告，数字表示只订阅榜单前 N 名缺失项。
