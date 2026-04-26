# Emby榜单合集同步

基于 `ffinly/emby-collection-sync` 移植的 MoviePilot V2 插件。

## 功能

- 同步 TMDb List 到 Emby BoxSet 合集。
- 支持 IMDb Top 250、豆瓣电影 Top 250、豆瓣 28 个分类榜单。
- 自动写入榜单简介、注入 TMDb 原语言海报。
- 支持国产电影/电视剧自动合集。
- 支持扫描并修复无封面的普通合集。
- 可将榜单缺失项直接加入 MoviePilot 订阅。

## 配置

安装后在插件配置页填写：

- Emby 地址与 API Key。
- TMDb v3 API Key。
- 是否启用代理。
- 执行周期，默认 `30 8 * * *`。
- 核心榜单 JSON 与分类榜单 JSON。

缺失项订阅使用 MoviePilot 内部 `SubscribeChain`，不再需要单独填写 MoviePilot URL/API Token。
