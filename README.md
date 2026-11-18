# orca-pocket

**Feishu ⇄ Orca** — 用飞书（Lark）把手机接到常年开机的电脑上，操控本机的 coding agent。

> 校园网封了 Tailscale？飞书事件走的是**出向 WebSocket 长连接**，不需要公网 IP、端口映射或隧道。


---

## 0. 前置：飞书应用配置

### 这台机器上已经有一份现成的

如果你之前折腾过别的飞书项目（机器人、妙搭应用等），`lark-cli` 里可能已经绑好了
一个 app。本项目**直接复用它，不需要新建机器人**——只要它有机器人能力且事件订阅走长连接。

自检：

```bash
lark-cli auth status           # bot 身份必须是 ready
python bridge.py --selftest    # 必须看到 feishu-websocket: connected
```

### 如果要从零配一个（或换台机器）

1. [open.feishu.cn](https://open.feishu.cn) → 创建**企业自建应用**
2. **应用功能 → 机器人** → 启用
3. **权限管理** → 添加：

   | scope | 用途 |
   |---|---|
   | `im:message.p2p_msg:readonly` | 接收私聊消息 |
   | `im:message:send_as_bot` | 以 bot 身份回复 |
   | `application:app_slash_command:write` | 注册 `/` 快捷指令（可选） |
   | `application:application:patch` | 同上，快捷指令接口的附带要求 |

4. **事件与回调 → 订阅方式** → 选 **长连接**（⚠️ 不要选「发送至开发者服务器」）
5. **事件与回调 → 已订阅事件** → 添加 `im.message.receive_v1`（接收消息）
6. **版本管理与发布** → 创建版本 → 申请发布（企业管理员审核）
7. **可用范围** → 至少包含自己
8. 本机绑定：`lark-cli auth login`

> ⚠️ 权限、事件订阅、可用范围任何一项改动，都必须**重新发版**才生效。
> 这是最容易踩的坑：后台看起来配好了，客户端却收不到消息。

### 快捷指令

已注册 6 条（`/wt` `/term` `/tail` `/ping` `/help` `/id`），飞书输入框里打 `/`
会弹出命令列表并带中文说明，不用记。

重新注册 / 新增：

```bash
lark-cli application +slash-command-create --as bot \
  --command wt --description "workspace 列表" \
  --description-i18n zh_cn=workspace列表 --force
lark-cli application +slash-command-list --as bot --json
```

需要 scope `application:app_slash_command:write` + `application:application:patch`，
且**必须重新发版**才生效。服务端立即更新，客户端有约 5 分钟缓存。

---

## 1. 产品定位：手机是监督层，不是操作层

**桌面端 = 执行面，手机端 = 收件箱 + 审批台。**

分工依据：

- 手机的优势是在线、能收推送、能做是非题；劣势是屏窄、输入慢、无法在两个长文本之间来回比对
- agent 的真实痛点不是"发不起任务"，而是"跑完了没人知道""卡在批准上没人管"
- 同类产品（CodeAgent Mobile）的定位同样是 *async AI coding supervision*，
  其创始人公开说最难的是给用户解释"为什么要在手机上写代码"——因为那不是卖点
- Orca 自己的移动端能力命名也是 `mobile.tasks.v1` / `notifications.remote-push.v1`

因此本项目的取舍：

- 默认只显示**需要你注意的东西**（失败 / 等待批准 / 刚完成），不是全量列表
- `/wt` 默认 5 条按最近活动排序，`/wt all` 才展开全部
- 输出走富文本（post + markdown），不是等宽表格——手机宽度放不下表格
- 后续方向是**主动推送 + 卡片按钮**，而不是把更多命令搬上来

---

## 2. 链路

```
手机飞书 → 飞书服务器 ──WSS 长连接(出向)──> bridge.py (Windows)
                                                │
                            lark-cli event consume im.message.receive_v1
                                                │  NDJSON on stdout
                                                ▼
                                     route() → 回复
                                                │
                            lark-cli im +messages-reply --as bot
```

## 3. 运行

```bash
python bridge.py            # 常驻
python bridge.py --selftest # 只验线路，不常驻
python bridge.py -v         # DEBUG 日志
```

## 4. 命令

| 命令 | 作用 |
|---|---|
| `/wt` | workspace 列表（最近 5 个，按活动排序） |
| `/wt all` | 全部 workspace |
| `/term` | 终端列表 |
| `/tail <n>` | 读 n 号终端的最后输出 |
| `/ping` | 探活 |
| `/id` | 你的 open_id / chat_id |
| `/help` | 命令帮助 |
| `/arm [分钟]` | 解锁写操作，默认 5 分钟 |
| `/use <n>` | 设定默认终端 |
| `/send <文本>` | 发给默认终端 |
| `/send <n> <文本>` | 发给 n 号终端 |
| `/disarm` | 立刻上锁 |

## 5. 里程碑

- [x] **1. 最小闭环** — 收消息 → 路由 → 回消息
- [x] **2. 只读面板** — `/wt` / `/term` / `/tail`
- [x] **2.1 手机可读性** — 富文本回复、按最近活动排序、默认截断
- [x] **2.2 快捷指令** — 6 条 `/` 命令注册到飞书输入框
- [x] **3. 混合执行** — `/arm` + `/use` + `/send` 写入终端
- [ ] **4. 主动推送** — agent 完成/卡住/push 失败时推卡片，带「看详情 / 继续 / 停止」按钮
- [ ] **5. 长输出** — 超长回复转飞书云文档，回链
- [ ] **6. 开机自启** — Windows 任务计划程序

## 6. 设计约定

**为什么不用 `lark-cli.cmd`**

`lark-cli` 是 npm 装的 shim。在 Windows 上 `subprocess` 调 `.cmd` 会经过 cmd.exe 二次解析参数，
agent 输出里带 `|`、`&`、`%`、`^` 或不成对的引号就会被改坏甚至当命令执行。
所以 `find_lark_cmd()` 优先直接 `node .../@larksuite/cli/scripts/run.js`，argv 原样传过去。

**为什么必须显式 `encoding="utf-8"`**

中文 Windows 上 `text=True` 默认用 cp936 解码子进程输出，中文回复全变豆腐块。
所有子进程调用都写死 UTF-8。

**关闭消费者进程**

`lark-cli event consume` 的 stdin 关闭 = 优雅退出；SIGTERM 安全；**绝不能 SIGKILL**
（会跳过服务端订阅注销，导致重启后重复投递 / "subscription already exists"）。
`EventConsumer.stop()` 先关 stdin，再 SIGTERM，从不 -9。

**事件流不能靠 deadline 读**

`for line in proc.stdout` 是阻塞读，没有事件时永远不返回——在循环里检查
`time.time() < deadline` 不会触发。需要退出就让子进程自己 `--timeout` 退出，
靠 stdout 关闭结束生成器。

**幂等**

按 `message_id` 去重（进程内 + 回复时作为 `--idempotency-key`），重复投递不会刷两次屏。

## 7. 安全

- `allowed_open_ids` 白名单，空数组 = 放行所有人（别这么配）。
- `/id` 对所有人开放，只为让你拿到正确的 open_id——飞书 open_id 是**按 app 维度**的，
  `lark-cli auth status` 里的不一定等于事件里看到的那个。
- 里程碑 3 起，任何会写入终端的路径都要先过白名单 + 显式 `/arm` 时间窗。

---

## 8. 隐私

这个项目同时接触两类个人数据：飞书的 `open_id` / `chat_id`，以及日志里的会话内容。
公开仓库前请确认：

- `config.json`（含真实 open_id）**已被 gitignore**，仓库里只有 `config.example.json`
- 日志只记录 ID 前 10 位（`mask()`），完整 ID 不落盘
- README / 代码里不出现真实 app_id、open_id、机器路径、个人项目名

自动校验：

```bash
python check_privacy.py        # 扫 git 会发布的文件
python check_privacy.py --all  # 连未跟踪文件一起扫
```

个人词表（真名、用户名、项目名）写在 `privacy.local.txt`，该文件已被忽略；
模板见 `privacy.example.txt`。

启用提交前拦截：

```bash
git config core.hooksPath .githooks
```
