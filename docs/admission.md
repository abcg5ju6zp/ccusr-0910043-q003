# 准入控制（Admission Control）基线

本基线描述 `sanic/admission/` 子系统：按**路由**与**调用方**配置并发席位、
排队上限与优先级，避免月末少数慢请求占满全部工作进程，同时保证健康检查、
退款确认等关键流量不被普通查询拖垮。

## 1. 要解决的问题

- 慢请求占满工作进程 → 普通查询集体超时；
- 简单限制全局并发 → 健康检查、退款确认等关键请求也无法及时执行；
- 需要按路由、按调用方分别配置席位 / 排队 / 优先级；
- 请求中间件可能改写路由，改写后只应按**最终路由**计费一次；
- 排队中断开、处理超时、客户端重试、应用停机、策略热切换都必须及时归还席位；
- 多个 worker 的本地判定必须汇总成稳定的“受理 / 拒绝”结论；
- 响应要能说明本次使用的准入版本，但不能泄露其他租户的负载。

## 2. 组件总览

```
sanic/admission/
├── policy.py       # AdmissionRule / AdmissionPolicy：规则、匹配、版本
├── gates.py        # AdmissionToken、LocalCoordinator、SharedState/Coordinator
├── controller.py   # AdmissionController：调用方识别 + Sanic 生命周期接线
└── exceptions.py   # AdmissionRejected / Timeout / Unavailable
```

- **席位桶（bucket）**：身份由“路由模式 + 调用方模式”决定
  （`bucket_key`），与额度数值无关。一条请求恰好落入一个桶。
- **策略快照（AdmissionPolicy）**：不可变规则集合 + 版本号。切换策略是
  整体替换快照，因此一个请求从受理到结束只按一个版本、一个桶计费。
- **协调器**：
  - `LocalCoordinator`：单 worker、单事件循环，纯本地结构，严格优先级 + FIFO；
  - `SharedCoordinator`：多 worker 共享 `SharedState`（`multiprocessing`
    共享数组 + 每槽条件变量），判定在共享锁内串行完成。

## 3. 规则与匹配

```python
AdmissionRule(
    routes="settle.refund",        # 路由名 glob；可用 ("a.*", "b.*")
    concurrency=4,                 # 同时执行席位（>0）
    callers="tenant-*",            # 调用方标识 glob，默认 "*"
    queue_limit=8,                 # 席位满后允许排队数；0 = 立即拒绝
    priority=10,                   # 排队调度优先级，越大越先得席位
    queue_timeout=2.0,             # 排队最长秒数；None = 仅受断连约束
    retry_after=1,                 # 被拒响应 Retry-After（秒）
    name="refund",
)
```

- 匹配优先级（`AdmissionPolicy.lookup`）：**调用方具体度 → 路由具体度 →
  声明顺序**，选出唯一一条规则。具体度：精确匹配 > 前缀通配（`x.*`）>
  其他通配；同级越长越具体。因此最具体规则确定胜出，不存在两条规则都
  “部分更具体”的歧义。
- 无任何规则命中的路由（如健康检查）**不消耗席位**，天然不受限额影响。
- 版本：不显式指定时，由规则内容（模式、席位、排队、优先级、超时）哈希
  生成 12 位版本；内容不变则版本稳定，可显式传 `version=` 固定。

## 4. 接入方式

### 4.1 单 worker / 单进程

```python
from sanic.admission import install_admission, AdmissionRule

install_admission(app, [
    AdmissionRule("app.health", 1000, name="health"),
    AdmissionRule("app.settle.refund", 16, callers="*", priority=10,
                   queue_limit=32, name="refund"),
    AdmissionRule("app.settle.*", 8, callers="*", queue_limit=16,
                   name="settle"),
], caller_header="x-caller")   # 也可用 caller_resolver= 自定义识别
```

也可以传入 `caller_resolver=lambda request: ...` 从令牌、主体、网关头
等位置识别调用方；解析失败回落到 `anonymous`。

### 4.2 多 worker 部署

```python
from sanic.admission import setup_shared_admission

controller = setup_shared_admission(
    app, rules,
    slot_count=64,      # 共享桶槽位数（需 >= 不同桶数量）
    entry_capacity=256, # 单槽条目表容量（需 >= 桶 concurrency+queue_limit）
)
```

入口在主进程 `main_process_start` 创建 `SharedState` 放入 `app.shared_ctx`
（fork / 继承给各 worker），每个 worker 在 `after_server_start` 绑定到同一
共享协调器。`AdmissionController.update_policy(...)` 在任一 worker 调用即可
把容量、排队上限、桶开关写入共享内存并广播，其他 worker 的后续判定立即按
新版本执行。

## 5. 请求生命周期与计费正确性

准入点挂在内建信号 **`http.handler.before`**——它在路由解析与**全部请求
中间件执行之后、业务处理器之前**触发。此时 `request.route` 已经是最终路由：

- 若中间件把 `/refund` 改写到 settle 路由，则请求只占用 **settle 桶**，
  不会先占 refund 再占 settle（不重复计费）；
- 每个请求只申请一次席位、只释放一次（令牌 `release` 幂等）。

席位释放构成多重保险，任一成立即可：

| 结束方式 | 释放路径 |
| --- | --- |
| 正常响应 / 错误响应已生成 | `http.lifecycle.response` 信号 |
| 响应无法发送（处理异常等） | `http.lifecycle.exception` 信号 |
| 传输已关闭、任务被取消/超时/停机 | 处理任务 `done_callback` 兜底 |
| 排队中客户端断开 | 等待任务 `CancelledError` → 直接归还排队名额 |

共享协调器的正常释放走 `release_async()`（等待 mp 锁操作真正落地），
保证“响应完成即席位归还”的确定性；任务回调兜底路径则是触发即忘，
绝不在断连路径上再次抛出。

- **排队超时**：本地用事件循环相对时钟 `call_later`；共享用条件变量带超时
  等待。超时返回 503，排队名额立即回收。
- **重试**：被拒（503）与排队超时从不占用席位，因此客户端重试不会累积占用。
- **停机**：`before_server_stop` 调用 `coordinator.shutdown()`，新申请立即
  失败、排队者立即被唤醒失败；在途席位继续优雅排空。`after_server_start`
  会 `open()`，进程内重启 / 测试生命周期后自动恢复。
- **策略切换**：新请求按新版本；被删除桶中的排队者立即失败；在途请求继续
  按取得席位时的桶计数，结束后归还到同一桶，计数不丢、不超额。

## 6. 多 worker 判定的稳定性

`SharedState` 每个桶槽位持有共享的 `active`、`queued`、`capacity`、
`queue_limit`、开关标志，以及一张定长排序队列表（优先级 + 全局取票号）
和一个条件变量。所有“受理 / 排队 / 拒绝”判定都在该槽位的同一把锁内完成：

- **不会超额受理**：`active < capacity` 与自增是原子的；
- **有名额不会误拒**：容量、排队上限也是共享值，判定不依赖各 worker
  本地缓存；
- **全局优先级**：等待条目按 `(priority 降序, 取票号升序)` 排序，跨 worker
  高优先级后到也先得席位，同级严格 FIFO 防饥饿；
- 阻塞原语在专用 `ThreadPoolExecutor` 中执行（池容量 ≥ 各桶排队上限之和 +
  槽位数），不占用默认线程池，也不卡住事件循环。

容量规划：`slot_count` 必须不小于策略中不同桶的数量，不同桶哈希到同一槽位
会在 `apply_policy` 时明确抛错；`entry_capacity` 必须不小于任一桶的
`concurrency + queue_limit`（同一张条目表要同时容纳在途 GRANTED 墓碑与
WAITING 排队条目）。

## 7. 响应契约

- 成功与被拒响应都带 `X-Admission-Version: <本次判定版本>`；
- 席位满且队列满 → `503 Service Unavailable`，正文仅为通用的
  “Admission capacity exceeded”，并带 `Retry-After`；
- 排队超时 → 503（“Timed out waiting for admission”），`Retry-After: 0`；
- 停机 → 503（“Service is shutting down”）；
- 版本头之外**不返回**任何按调用方/租户维度的占用、队列等负载数据；
  可观测接口 `controller.occupancy(rule)` 只供本进程运维使用，不进响应。

## 8. 测试基线

`tests/test_admission.py`（31 个用例）覆盖：

- 规则校验、版本确定性/唯一性、具体度匹配、glob；
- 本地：席位上限与幂等释放、FIFO 转移计数、严格优先级、排队超时、
  排队中取消归还、停机拒绝/重开、扩容即时放行、删桶中断排队；
- 共享（多 worker 线程 + 独立事件循环）：30 并发恰好受理
  `席位+排队` 个且峰值不超、跨 worker 优先级、排队超时清理、容量热切换
  与删桶、槽位冲突检测、停机广播与重开；
- Sanic 集成：版本头、503 + Retry-After、无规则路由不计费、健康检查
  不被阻塞、按调用方独立桶、中间件改写路由只按最终路由计费、排队超时
  释放、策略热切换版本头生效、断连（排队中与处理中）归还、被拒后重试
  成功、停机生命周期接线。

运行：`python3 -m pytest tests/test_admission.py -q`

## 9. 边界与注意事项

- 准入在 `http.handler.before` 生效，保护的是**业务处理器并发**；请求体
  预读、路由解析本身不受席位约束（这是刻意的，避免慢请求在更早阶段堆积时
  反而无法被准入统计）。
- `queue_limit=0` 表示不排队、立即拒绝；想要“宁可等待不要拒绝”应给正的
  `queue_limit` 并按需设置 `queue_timeout`。
- 共享协调器依赖 `multiprocessing` 共享内存，仅在多 worker（fork/继承）
  部署下使用；单进程 / ASGI / 测试默认使用 `LocalCoordinator`。
- 调用方标识来自可配置的请求头或解析器，**准入不做身份认证**；应在更前面
  的中间件完成认证后再把可信标识交给 `caller_resolver`。
