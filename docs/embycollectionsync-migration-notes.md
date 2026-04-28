# Emby Collection Sync 移植说明

参考仓库：

- https://github.com/ffinly/emby-collection-sync
- https://github.com/jxxghp/MoviePilot
- https://github.com/jxxghp/MoviePilot-Plugins

## 已调整内容

- 原脚本的硬编码常量改为插件配置页字段。
- 原青龙 `notify.send` 改为 MoviePilot `post_message`。
- 原青龙 cron 改为 `get_service()` 注册 MoviePilot 公共定时服务。
- 原 MoviePilot HTTP API 调用改为插件内直接使用 `SubscribeChain` 添加订阅。
- 保留 Emby HTTP API、TMDb List 拉取、合集重建、海报注入、国产合集与无封面修复逻辑。

## 2026-04-28 更新

- 拉取 `ffinly/emby-collection-sync` 最新提交 `e25c916`。
- 同步朋友项目最新榜单：10 个核心榜单、84 个豆瓣分类榜单。
- 将榜单配置拆为“内置榜单多选”和“自定义合集榜单 JSON”。
- `mp_subscribe` 支持 `true`、`false`、整数排名阈值。
- 新增电影/剧集 TMDb ID 订阅排除列表。
- 新增榜单/国产合集海报抓取策略配置。

## 后续验证建议

- 在真实 MoviePilot V2 宿主中安装本地插件库。
- 先关闭“缺失项加入MP订阅”，只勾选“立即运行一次”验证 Emby 合集创建。
- 确认合集、封面与报告正常后，再开启订阅功能。
