# 闸北水工·启闭日志双写巡检

在值守交班前识别撕裂、旧代或双损日志页，生成可修复与隔离清单。

## 业务流程

1. 启闭值守
2. 故障页清单
3. 双写复核
4. 修复隔离单

字节级部分写入与页号、代次、内容校验，按已知持久线选择可恢复页。

闸北水工的 9 人值守及 DBA 小组以前凭日志拷贝时间判定恢复顺序。撕裂页能打开但内容错误，旧代副本经常通过校验，同代副本冲突不能靠时间戳解释。上游是已脱敏的闸控日志页和故障注入输入，下游是负责修复隔离审批的当班主管。

07:10–07:15 采集 N1 的 021–025 页；07:18–07:23 录入第二版启闭日志；07:25 按故障发生阶段造出二进制页；07:27 核对页号、代次和 SHA-256；07:28 检查退代拒绝；07:30 查看 N1-021 修复文本；07:35 为 N1-023 与 025 导出隔离证据。

模拟页通过页号、代次、正文长度和字节内容计算校验和。双写持久化之后断电，可从正确代次的双写页恢复损坏目标页；持久前断电则保留前代。部分写入后若目标字节恰好与完整新版一致，校验通过并保留目标页。假设数据库已提供可信的持久代。模拟器在内存中选择可读取的页内容，MySQL 保存批单和复核结果。

## 启动

```sh
cp -n .env.example .env
docker compose config --quiet
docker compose up -d --build --wait
```

打开 <http://localhost:18257>。OpenAPI 在 <http://localhost:19257/docs>，数据库映射到本机 17757。所有暴露端口只绑定回环地址。Compose 启动 frontend、backend、worker、db、redis，各自通过健康探针检查。数据库、Redis 和共享产物目录各有命名卷；任务状态及导出内容以数据库中的证据为准。

首次启动时幂等载入演示数据。手动载入可运行 `docker compose exec -T backend python -m scripts.seed_demo`。常规停止可用 `docker compose down`。验收环境清理才加 `-v`，它会删除本项目的数据卷。

## 角色与交接

演示账号共用口令 `Demo-DbWorkbench-2026`。

| 登录名 | 角色 | 可执行操作 |
|---|---|---|
| operator | 资料操作员 | 登记业务资料、设置「撕裂写入前缀（字节，1–128）」、提交、查看和下载结果 |
| reviewer | 业务复核员 | 查看执行证据、签署单次复核、下载交接单 |
| viewer | 只读观察员 | 查看资料、执行结果及审计，下载证据 |

选用「GT-N1-1124 / 北一闸」对应的演示资料，保存一份复演资料，再以默认参数 6 提交。worker 保存结果摘要、业务明细和 SHA-256。复核员登录后在「修复隔离单」签署。

## 开发说明

| 部分 | 实现 |
|---|---|
| 前端 | 原生 HTML、CSS、JavaScript ES Modules、Nginx |
| 后端 | Python 3.11、FastAPI、SQLAlchemy、JWT |
| 数据库 | MySQL 8.0 |
| 后台执行 | Redis 通知、数据库持久任务队列和行锁、Python worker |

### 巡检中断与接手续跑

巡检按页落库，worker 进程失联后另一个 worker 可以接手同一批次，而不是从头再来：

- 批次状态为 `queued → running → done/failed`。worker 领取批次时写入归属（`owner_id`）并把 `owner_epoch` 加一，该代次作为 fencing token；每次写页、续租或改批次状态都要求 `(owner_id, owner_epoch)` 仍是当前持有者，旧进程恢复后代次已失效，任何写页或状态变更都匹配零行而被拒绝。
- 每个 worker 进程有唯一 `WORKER_ID`，处理期间按 `HEARTBEAT_EVERY`（10 秒）续租 `lease_expires_at`（租期 30 秒）。慢但仍有心跳的进程持续续租，批次不会被误接手；停止心跳超过租期的批次，其他 worker 可在队列扫描时接手。
- 每页结果独立事务写入 `run_pages`，`(run_id, page_key)` 唯一约束保证中断重试不重复写已落库的页；接手方只补齐缺失页。
- 只有全部页落齐后，才在同一事务汇总 `result`、计算 `digest` 并置为 `done`（生成批次证据），证据与状态不会出现半成品；其他批次互不影响、照常办理。
- 接手轨迹写入审计（`claim`、`handoff`、`done`、`failed`）。巡检页面显示进度（已落库页数/总页数）、原进程与当前进程，接手并续跑完成时明确提示「原批次已接手并续跑完成」。

后台故障转移的协议级测试在 `backend/tests/`（基于内存 SQLite 跑真实 SQLAlchemy 服务代码）：

```sh
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python tests/test_failover.py
python tests/test_api_smoke.py
```


从 `backend/src/domain/scenario.py` 修改业务入口和演示口径，运行 `docker compose up -d --build --wait` 查看变更。数据库表结构在 `backend/src/models` 和 `backend/migrations`，输入模型在 `backend/src/schemas`。业务页和请求接口在 `frontend/src/pages`、`frontend/src/api`。资料、执行、复核三个工作流实体各有独立表和 API；场景输入及结果保留在所属资料和批次中。

角色枚举在 `backend/src/constants/roles.py`、`frontend/src/constants/roles.js`，用于服务端访问控制、账号初始化、前端导航与签署。批次状态在 `backend/src/constants/job_state.py`、`frontend/src/constants/states.js`，用于队列、状态 API、状态标记与轮询，包含等待执行（queued）、巡检中（running，带租约归属）、已完成（done）与执行失败（failed）。登记、领取、接手、完成和签署事件写入审计表。

`COMPOSE_PROJECT_NAME` 设置栈名，`FRONTEND_PORT`、`BACKEND_PORT`、`DB_PORT` 设置端口。通过 `DB_PASSWORD`、`JWT_SECRET`、`DEMO_PASSWORD` 配置本地凭据。设置 `SEED_DEMO=false` 可关闭演示数据载入，此时数据库初始没有演示账号。数据库密码调整后，已有数据卷需在数据库内同步修改用户密码。

## 验收范围

合成页与撕裂故障用于复演，恢复文本和隔离结果作为证据。实际数据库存储由 MySQL 承担。 当前界面用 JSON 编辑业务资料，以 JSON 下载证据。过程和截图保存在批次的 [验收记录](../../test-artifacts/gb-6108/TESTING.md)。

[MIT License](LICENSE)
