# node

`vortex-node`：Agent + 嵌入的 Engine（开发方案 02）。当前进度见 `docs/m3-progress.md`。

| 目录 | 内容 |
| --- | --- |
| `agent/store` | 本地持久化（AGT-003）。Go 模块代理放行前为只追加 JSON 行文件，之后换 SQLite（WAL） |
| `agent/boot` | 启动标识：每次启动新 boot_id，记住上一次（AGT-009） |
| `agent/report` | 计量落盘、封批、确认、重发、7 天保留；磁盘水位保护（AGT-006、MTR-006） |
| `agent/grants` | 授权与份额：续期、计费授权申请、只收当前实例签发的授权、切换实例后报告持有授权（AGT-005、HUB-014、ENG-050） |
| `internal/clock` | 可注入时钟 |
