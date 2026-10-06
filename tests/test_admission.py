import asyncio
import contextlib
import threading
import time

import pytest

from sanic import Sanic
from sanic.admission import (
    AdmissionPolicy,
    AdmissionRule,
    AdmissionToken,
    LocalCoordinator,
    SharedCoordinator,
    SharedState,
    bucket_key,
    install_admission,
)
from sanic.admission.exceptions import (
    AdmissionRejected,
    AdmissionTimeout,
    AdmissionUnavailable,
)
from sanic.response import text


# SanicASGITestClient 会把类级 Sanic.__call__ 替换成 app_call_with_return
# 且不还原，使本模块之后运行的真实 uvicorn 子进程测试走错 ASGI 入口、
# 生命周期监听器不触发。在模块导入时（早于任何 asgi_client 使用）保存
# 原始方法，供隔离夹具还原。
_PRISTINE_SANIC_CALL = Sanic.__call__
assert getattr(_PRISTINE_SANIC_CALL, "__qualname__", "") == "Sanic.__call__"


@pytest.fixture(autouse=True)
def _isolate_sanic_state():
    """隔离本模块直接构造的 Sanic 应用、类级 __call__ 补丁与共享线程池。

    本文件部分用例不经过 conftest 的 ``app`` 夹具，因此需要自行复刻其
    清理：还原 Sanic.__call__、清空全局应用注册表（否则污染后续
    test_asgi 等模块），并回收 SharedCoordinator 的专用线程池。
    """
    import gc

    Sanic.__call__ = _PRISTINE_SANIC_CALL
    yield
    gc.collect()
    for obj in gc.get_objects():
        if isinstance(obj, SharedCoordinator) and obj._executor is not None:
            obj.close_executor()
    Sanic._app_registry.clear()
    Sanic.__call__ = _PRISTINE_SANIC_CALL


# ---------------------------------------------------------------------- #
# 策略模型
# ---------------------------------------------------------------------- #


def test_rule_validation():
    with pytest.raises(ValueError):
        AdmissionRule("", 1)
    with pytest.raises(ValueError):
        AdmissionRule("r", 0)
    with pytest.raises(ValueError):
        AdmissionRule("r", 1, queue_limit=-1)
    with pytest.raises(ValueError):
        AdmissionRule("r", 1, queue_timeout=0)


def test_policy_version_is_deterministic():
    rules = [
        AdmissionRule("settle.*", 4, callers="*", queue_limit=2),
        AdmissionRule("settle.refund", 10, callers="ops"),
    ]
    p1 = AdmissionPolicy.create(rules)
    p2 = AdmissionPolicy.create(list(rules))
    assert p1.version == p2.version
    p3 = AdmissionPolicy.create(
        [AdmissionRule("settle.*", 5, callers="*", queue_limit=2)]
    )
    assert p3.version != p1.version
    pinned = AdmissionPolicy.create(rules, version="v-2026-10")
    assert pinned.version == "v-2026-10"


def test_policy_specificity_exact_beats_glob():
    policy = AdmissionPolicy.create(
        [
            AdmissionRule("settle.*", 4, name="wild"),
            AdmissionRule("settle.refund", 10, name="refund"),
        ]
    )
    assert policy.lookup("settle.refund", "anyone").name == "refund"
    assert policy.lookup("settle.query", "anyone").name == "wild"
    assert policy.lookup("health", "anyone") is None


def test_policy_caller_specificity_then_route():
    policy = AdmissionPolicy.create(
        [
            AdmissionRule("settle.*", 4, callers="*", name="any"),
            AdmissionRule("settle.*", 2, callers="tenant-a", name="a"),
            AdmissionRule(
                "settle.refund", 8, callers="tenant-b", name="b-refund"
            ),
        ]
    )
    assert policy.lookup("settle.query", "tenant-a").name == "a"
    assert policy.lookup("settle.query", "tenant-c").name == "any"
    assert policy.lookup("settle.refund", "tenant-b").name == "b-refund"
    # 精确调用方 + glob 路由 优于 通配调用方 + 精确路由
    policy2 = AdmissionPolicy.create(
        [
            AdmissionRule("settle.refund", 8, callers="*", name="exact-route"),
            AdmissionRule(
                "settle.*", 8, callers="tenant-b", name="exact-caller"
            ),
        ]
    )
    assert policy2.lookup("settle.refund", "tenant-b").name == "exact-caller"


def test_policy_glob_matching():
    policy = AdmissionPolicy.create(
        [AdmissionRule(("api.v1.*", "api.v2.*"), 3, name="api")]
    )
    assert policy.lookup("api.v1.settle", "c").name == "api"
    assert policy.lookup("api.v2.refund", "c").name == "api"
    assert policy.lookup("api.v3.settle", "c") is None


# ---------------------------------------------------------------------- #
# 本地协调器
# ---------------------------------------------------------------------- #


def make_local(rules):
    policy = AdmissionPolicy.create(rules, version="v")
    coord = LocalCoordinator()
    coord.apply_policy(policy)
    return policy, coord


async def acquire(coord, rule, policy, monotonic=lambda: 0.0):
    token = AdmissionToken(bucket_key(rule), policy.version, rule=rule)
    await coord.wait_for_seat(
        bucket_key(rule), rule, token, monotonic=monotonic
    )
    return token


@pytest.mark.asyncio
async def test_local_concurrency_and_release():
    rule = AdmissionRule("settle.*", 2, queue_limit=0, name="s")
    policy, coord = make_local([rule])
    key = bucket_key(rule)

    t1 = await acquire(coord, rule, policy)
    t2 = await acquire(coord, rule, policy)
    assert coord.active(key) == 2
    with pytest.raises(AdmissionRejected):
        await acquire(coord, rule, policy)
    coord.release(t1)
    assert coord.active(key) == 1
    coord.release(t2)
    assert coord.active(key) == 0
    # 释放幂等
    coord.release(t1)
    assert coord.active(key) == 0
    # 席位全部归还后可再次受理
    t3 = await acquire(coord, rule, policy)
    assert t3.granted


@pytest.mark.asyncio
async def test_local_queue_grant_is_fifo_and_accounts_seats():
    rule = AdmissionRule("settle.*", 1, queue_limit=2, name="s")
    policy, coord = make_local([rule])
    key = bucket_key(rule)

    holder = await acquire(coord, rule, policy)
    order = []

    async def enqueue(label):
        token = AdmissionToken(key, policy.version, rule=rule)
        await coord.wait_for_seat(key, rule, token, monotonic=lambda: 0.0)
        order.append(label)
        await asyncio.sleep(0.02)
        coord.release(token)

    first = asyncio.ensure_future(enqueue("first"))
    second = asyncio.ensure_future(enqueue("second"))
    await asyncio.sleep(0.02)
    assert coord.waiting(key) == 2

    # 队列已满（queue_limit=2），第三个请求立即被拒
    with pytest.raises(AdmissionRejected):
        await acquire(coord, rule, policy)

    # 释放持有者后，席位依次沿 FIFO 队列转移（每个自取后自行释放）
    coord.release(holder)
    await asyncio.gather(first, second)
    assert order == ["first", "second"]
    assert coord.active(key) == 0
    assert coord.waiting(key) == 0


@pytest.mark.asyncio
async def test_local_priority_overtakes_on_release():
    low = AdmissionRule("r.*", 1, queue_limit=4, priority=0, name="lo")
    high = AdmissionRule("r.*", 1, queue_limit=4, priority=10, name="hi")
    policy, coord = make_local([low])
    key = bucket_key(low)

    holder = await acquire(coord, low, policy)
    order = []

    async def enqueue(rule, label):
        token = AdmissionToken(key, policy.version, rule=rule)
        try:
            await coord.wait_for_seat(key, rule, token, monotonic=lambda: 0.0)
            order.append(label)
            # 拿到席位后短暂占用，保证顺序可观察
            await asyncio.sleep(0.05)
            coord.release(token)
        except Exception:
            pass

    t_low = asyncio.ensure_future(enqueue(low, "low"))
    await asyncio.sleep(0.02)
    t_high = asyncio.ensure_future(enqueue(high, "high"))
    await asyncio.sleep(0.02)
    assert coord.waiting(key) == 2

    # 释放占席者：高优先级后到但应先得席位
    coord.release(holder)
    await asyncio.gather(t_high, t_low)
    assert order == ["high", "low"]
    assert coord.active(key) == 0
    assert coord.waiting(key) == 0


@pytest.mark.asyncio
async def test_local_queue_timeout_releases_slot():
    rule = AdmissionRule("r.*", 1, queue_limit=2, queue_timeout=0.2, name="r")
    policy, coord = make_local([rule])
    key = bucket_key(rule)

    holder = await acquire(coord, rule, policy, monotonic=time.monotonic)

    async def park():
        token = AdmissionToken(key, policy.version, rule=rule)
        await coord.wait_for_seat(key, rule, token, monotonic=time.monotonic)

    task = asyncio.ensure_future(park())
    await asyncio.sleep(0.05)
    assert coord.waiting(key) == 1
    with pytest.raises(AdmissionTimeout):
        await asyncio.wait_for(task, timeout=1.0)
    assert coord.waiting(key) == 0
    coord.release(holder)
    assert coord.active(key) == 0
    # 超时后名额已归还，新请求可占席位
    again = await acquire(coord, rule, policy, monotonic=time.monotonic)
    assert again.granted


@pytest.mark.asyncio
async def test_local_cancel_while_queued_returns_queue_slot():
    rule = AdmissionRule("r.*", 1, queue_limit=1, name="r")
    policy, coord = make_local([rule])
    key = bucket_key(rule)

    holder = await acquire(coord, rule, policy)

    async def park():
        token = AdmissionToken(key, policy.version, rule=rule)
        await coord.wait_for_seat(key, rule, token, monotonic=lambda: 0.0)

    queued = asyncio.ensure_future(park())
    await asyncio.sleep(0.02)
    assert coord.waiting(key) == 1

    # 队列已满，新请求被拒
    with pytest.raises(AdmissionRejected):
        await acquire(coord, rule, policy)

    queued.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await queued
    await asyncio.sleep(0.02)
    # 取消后排队名额归还，新请求可以排队
    replacement = asyncio.ensure_future(park())
    await asyncio.sleep(0.02)
    assert coord.waiting(key) == 1
    replacement.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await replacement
    coord.release(holder)
    assert coord.active(key) == 0
    assert coord.waiting(key) == 0


@pytest.mark.asyncio
async def test_local_shutdown_aborts_waiters_and_rejects_new():
    rule = AdmissionRule("r.*", 1, queue_limit=3, name="r")
    policy, coord = make_local([rule])
    key = bucket_key(rule)
    holder = await acquire(coord, rule, policy)

    async def park():
        token = AdmissionToken(key, policy.version, rule=rule)
        await coord.wait_for_seat(key, rule, token, monotonic=lambda: 0.0)

    queued = asyncio.ensure_future(park())
    await asyncio.sleep(0.02)
    coord.shutdown()
    with pytest.raises(AdmissionUnavailable):
        await queued
    with pytest.raises(AdmissionUnavailable):
        await acquire(coord, rule, policy)
    # 在途席位不受影响，可正常归还
    coord.release(holder)
    assert coord.active(key) == 0
    # 重新开放（进程内重启）
    coord.open()
    again = await acquire(coord, rule, policy)
    assert again.granted


@pytest.mark.asyncio
async def test_local_policy_change_adjusts_capacity():
    r1 = AdmissionRule("r.*", 1, queue_limit=4, name="r")
    p1 = AdmissionPolicy.create([r1], version="v1")
    coord = LocalCoordinator()
    coord.apply_policy(p1)
    key = bucket_key(r1)
    holder = await acquire(coord, r1, p1)

    granted_tokens = []

    async def park():
        token = AdmissionToken(key, "v1", rule=r1)
        await coord.wait_for_seat(key, r1, token, monotonic=lambda: 0.0)
        granted_tokens.append(token)

    q1 = asyncio.ensure_future(park())
    q2 = asyncio.ensure_future(park())
    await asyncio.sleep(0.02)
    assert coord.waiting(key) == 2

    p2 = AdmissionPolicy.create(
        [AdmissionRule("r.*", 3, queue_limit=4, name="r")], version="v2"
    )
    coord.apply_policy(p2)
    await asyncio.gather(q1, q2)
    assert len(granted_tokens) == 2
    assert coord.active(key) == 3
    assert coord.waiting(key) == 0
    coord.release(holder)
    for token in granted_tokens:
        coord.release(token)
    assert coord.active(key) == 0
    assert coord.waiting(key) == 0


@pytest.mark.asyncio
async def test_local_policy_change_removing_bucket_closes_it():
    r1 = AdmissionRule("r.*", 1, queue_limit=4, name="r")
    p1 = AdmissionPolicy.create([r1], version="v1")
    coord = LocalCoordinator()
    coord.apply_policy(p1)
    key = bucket_key(r1)
    holder = await acquire(coord, r1, p1)

    async def park():
        token = AdmissionToken(key, "v1", rule=r1)
        await coord.wait_for_seat(key, r1, token, monotonic=lambda: 0.0)

    queued = asyncio.ensure_future(park())
    await asyncio.sleep(0.02)

    p2 = AdmissionPolicy.create(
        [AdmissionRule("other.*", 1, queue_limit=0, name="o")], version="v2"
    )
    coord.apply_policy(p2)
    with pytest.raises(AdmissionUnavailable):
        await queued
    # 在途请求继续持有，结束后归还，计数不丢
    assert coord.active(key) == 1
    coord.release(holder)
    assert coord.active(key) == 0


# ---------------------------------------------------------------------- #
# 多 worker 共享协调器（每线程一个 worker：独立事件循环 + 独立协调器）
# ---------------------------------------------------------------------- #


def run_in_worker_thread(state, policy, rule, hold, outcomes, barrier):
    coord = SharedCoordinator(state)
    coord.apply_policy(policy)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    token = AdmissionToken(bucket_key(rule), policy.version, rule=rule)
    try:
        loop.run_until_complete(
            coord.wait_for_seat(
                bucket_key(rule), rule, token, monotonic=time.monotonic
            )
        )
        outcomes.append("accepted")
        time.sleep(hold)
        loop.run_until_complete(coord.release_async(token))
    except AdmissionRejected:
        outcomes.append("rejected")
    except AdmissionTimeout:
        outcomes.append("timeout")
    except AdmissionUnavailable:
        outcomes.append("unavailable")
    finally:
        loop.close()


def test_shared_stable_decisions_across_workers():
    concurrency, queue_limit, total = 3, 2, 30
    state = SharedState(slot_count=8, entry_capacity=64)
    rule = AdmissionRule(
        "settle.*", concurrency, queue_limit=queue_limit, name="s"
    )
    policy = AdmissionPolicy.create([rule], version="v")
    # 预建每个 worker 自己的协调器
    coordinators = [SharedCoordinator(state) for _ in range(total)]
    for coord in coordinators:
        coord.apply_policy(policy)

    outcomes = []
    lock = threading.Lock()
    peak = {"value": 0}
    barrier = threading.Barrier(total)

    def run(i):
        coord = coordinators[i]
        barrier.wait()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        token = AdmissionToken(bucket_key(rule), "v", rule=rule)
        try:
            loop.run_until_complete(
                coord.wait_for_seat(
                    bucket_key(rule),
                    rule,
                    token,
                    monotonic=time.monotonic,
                )
            )
            with lock:
                outcomes.append("accepted")
                active, _ = coordinators[0].snapshot(bucket_key(rule))
                peak["value"] = max(peak["value"], active)
            time.sleep(0.2)
            loop.run_until_complete(coord.release_async(token))
        except AdmissionRejected:
            with lock:
                outcomes.append("rejected")
        finally:
            loop.close()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(total)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert outcomes.count("accepted") == concurrency + queue_limit
    assert outcomes.count("rejected") == total - concurrency - queue_limit
    assert peak["value"] <= concurrency
    assert coordinators[0].snapshot(bucket_key(rule)) == (0, 0)


def test_shared_cross_worker_priority():
    state = SharedState(slot_count=4, entry_capacity=16)
    low = AdmissionRule("r.*", 1, queue_limit=4, priority=0, name="lo")
    high = AdmissionRule("r.*", 1, queue_limit=4, priority=10, name="hi")
    p_low = AdmissionPolicy.create([low], version="l")
    p_high = AdmissionPolicy.create([high], version="h")
    key = bucket_key(low)
    hold = threading.Event()
    order = []
    lock = threading.Lock()

    def run(rule, policy, label, do_hold):
        coord = SharedCoordinator(state)
        coord.apply_policy(policy)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        token = AdmissionToken(key, policy.version, rule=rule)
        try:
            loop.run_until_complete(
                coord.wait_for_seat(key, rule, token, monotonic=time.monotonic)
            )
            with lock:
                order.append(label)
            if do_hold:
                hold.wait()
                time.sleep(0.2)
            else:
                time.sleep(0.2)
            loop.run_until_complete(coord.release_async(token))
        except Exception:
            pass
        finally:
            loop.close()

    seat = threading.Thread(target=run, args=(low, p_low, "seat", True))
    seat.start()
    time.sleep(0.3)
    q_low = threading.Thread(target=run, args=(low, p_low, "low", False))
    q_low.start()
    time.sleep(0.3)
    q_high = threading.Thread(target=run, args=(high, p_high, "high", False))
    q_high.start()
    time.sleep(0.3)
    hold.set()
    seat.join()
    q_low.join()
    q_high.join()
    assert order == ["seat", "high", "low"]
    assert SharedCoordinator(state).snapshot(key) == (0, 0)


def test_shared_queue_timeout_and_cleanup():
    state = SharedState(slot_count=4, entry_capacity=16)
    rule = AdmissionRule("r.*", 1, queue_limit=2, queue_timeout=0.3, name="r")
    policy = AdmissionPolicy.create([rule], version="v")
    key = bucket_key(rule)
    outcomes = []

    def run(hold):
        coord = SharedCoordinator(state)
        coord.apply_policy(policy)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        token = AdmissionToken(key, "v", rule=rule)
        try:
            loop.run_until_complete(
                coord.wait_for_seat(key, rule, token, monotonic=time.monotonic)
            )
            outcomes.append("accepted")
            if hold:
                time.sleep(0.9)
            loop.run_until_complete(coord.release_async(token))
        except AdmissionTimeout:
            outcomes.append("timeout")
        finally:
            loop.close()

    holder = threading.Thread(target=run, args=(True,))
    holder.start()
    time.sleep(0.3)
    queued = threading.Thread(target=run, args=(False,))
    queued.start()
    holder.join()
    queued.join()
    assert "timeout" in outcomes
    assert SharedCoordinator(state).snapshot(key) == (0, 0)


def test_shared_policy_switch_capacity_and_close():
    state = SharedState(slot_count=4, entry_capacity=16)
    r1 = AdmissionRule("settle.*", 1, queue_limit=4, name="s")
    p1 = AdmissionPolicy.create([r1], version="v1")
    key = bucket_key(r1)
    main = SharedCoordinator(state)
    main.apply_policy(p1)
    results = {}

    def run(name):
        coord = SharedCoordinator(state)
        coord.apply_policy(p1)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        token = AdmissionToken(key, "v1", rule=r1)
        try:
            loop.run_until_complete(
                coord.wait_for_seat(key, r1, token, monotonic=time.monotonic)
            )
            results[name] = "in"
            time.sleep(1.5)
            loop.run_until_complete(coord.release_async(token))
        except AdmissionUnavailable:
            results[name] = "closed"
        finally:
            loop.close()

    holder = threading.Thread(target=run, args=("holder",))
    holder.start()
    time.sleep(0.3)
    q1 = threading.Thread(target=run, args=("q1",))
    q2 = threading.Thread(target=run, args=("q2",))
    q1.start()
    q2.start()
    time.sleep(0.3)
    assert main.snapshot(key) == (1, 2)

    p2 = AdmissionPolicy.create(
        [AdmissionRule("settle.*", 3, queue_limit=4, name="s")],
        version="v2",
    )
    main.apply_policy(p2)
    time.sleep(0.3)
    assert main.snapshot(key) == (3, 0)

    # 删除该桶：新请求被拒
    p3 = AdmissionPolicy.create(
        [AdmissionRule("other.*", 5, name="o")], version="v3"
    )
    main.apply_policy(p3)
    token = AdmissionToken(key, "v3", rule=r1)
    loop = asyncio.new_event_loop()
    with pytest.raises(AdmissionUnavailable):
        loop.run_until_complete(
            main.wait_for_seat(key, r1, token, monotonic=time.monotonic)
        )
    loop.close()

    holder.join()
    q1.join()
    q2.join()
    time.sleep(0.2)
    assert main.snapshot(key) == (0, 0)


def test_shared_slot_collision_detected():
    state = SharedState(slot_count=1, entry_capacity=8)
    coord = SharedCoordinator(state)
    # 两个不同桶映射到同一个槽位：明确报错而不是互相串用额度
    p = AdmissionPolicy.create(
        [
            AdmissionRule("alpha.*", 1, name="a"),
            AdmissionRule("beta.*", 1, name="b"),
        ]
    )
    with pytest.raises(RuntimeError):
        coord.apply_policy(p)


# ---------------------------------------------------------------------- #
# Sanic 集成
# ---------------------------------------------------------------------- #


def build_app(name="admission_it"):
    app = Sanic(name)

    @app.get("/health")
    async def health(request):
        return text("ok")

    @app.get("/settle")
    async def settle(request):
        await asyncio.sleep(0.4)
        return text("settled")

    @app.get("/refund")
    async def refund(request):
        return text("refunded")

    return app


@pytest.mark.asyncio
async def test_admission_version_header_on_success():
    app = build_app("version_success")
    controller = install_admission(
        app, [AdmissionRule("version_success.settle", 2, name="s")]
    )
    _, response = await app.asgi_client.get("/settle")
    assert response.status == 200
    assert response.headers["x-admission-version"] == controller.version


@pytest.mark.asyncio
async def test_admission_reject_is_503_with_version_and_retry_after():
    app = build_app("reject_503")
    controller = install_admission(
        app,
        [
            AdmissionRule(
                "reject_503.settle",
                1,
                queue_limit=0,
                retry_after=7,
                name="s",
            )
        ],
    )

    async def call_settle():
        return await app.asgi_client.get("/settle")

    in_flight = asyncio.ensure_future(call_settle())
    await asyncio.sleep(0.25)
    _, response = await call_settle()
    assert response.status == 503
    assert response.headers["x-admission-version"] == controller.version
    assert response.headers["retry-after"] == "7"
    # 正文只说明通用容量原因，不含任何调用方/租户负载信息
    assert "tenant" not in response.text.lower()
    assert "acct" not in response.text.lower()
    await in_flight


@pytest.mark.asyncio
async def test_unmatched_route_consumes_no_seat():
    app = build_app("unmatched_route")
    controller = install_admission(
        app, [AdmissionRule("unmatched_route.settle", 1, name="s")]
    )
    rule = controller.policy.lookup("unmatched_route.settle", "anonymous")
    # 健康检查无规则命中，不进任何席位桶，也带出版本头
    _, response = await app.asgi_client.get("/health")
    assert response.status == 200
    assert controller.occupancy(rule) == (0, 0)


@pytest.mark.asyncio
async def test_health_not_blocked_when_settle_full():
    app = build_app("health_isolation")
    install_admission(
        app,
        [
            AdmissionRule("health_isolation.settle", 1, queue_limit=0),
            AdmissionRule("health_isolation.health", 100),
        ],
    )

    async def call(path, caller=None):
        headers = {"x-caller": caller} if caller else {}
        _, r = await app.asgi_client.get(path, headers=headers)
        return r.status

    busy = asyncio.ensure_future(call("/settle"))
    await asyncio.sleep(0.25)
    assert await call("/settle") == 503
    # 健康检查必须照常受理
    assert await call("/health") == 200
    await busy


@pytest.mark.asyncio
async def test_per_caller_independent_buckets():
    app = build_app("per_caller")
    install_admission(
        app,
        [
            AdmissionRule(
                "per_caller.settle", 1, callers="acct-a", queue_limit=0
            ),
            AdmissionRule(
                "per_caller.settle", 1, callers="acct-b", queue_limit=0
            ),
            AdmissionRule("per_caller.settle", 1, callers="*", queue_limit=0),
        ],
        caller_header="x-caller",
    )

    async def call(caller):
        _, r = await app.asgi_client.get(
            "/settle", headers={"x-caller": caller}
        )
        return r.status

    a = asyncio.ensure_future(call("acct-a"))
    b = asyncio.ensure_future(call("acct-b"))
    await asyncio.sleep(0.3)
    assert await call("acct-a") == 503
    assert await call("acct-b") == 503
    # 第三个调用方使用独立的通配桶，互不影响
    assert await call("acct-c") == 200
    await asyncio.gather(a, b)


@pytest.mark.asyncio
async def test_route_rewritten_by_middleware_charged_to_final_route():
    app = build_app("route_rewrite")
    controller = install_admission(
        app,
        [
            AdmissionRule("route_rewrite.settle", 1, queue_limit=0),
            AdmissionRule("route_rewrite.refund", 100, queue_limit=0),
        ],
    )

    @app.on_request
    async def rewrite(request):
        if request.path == "/refund":
            route, _, _ = app.router.get("/settle", "GET", None)
            request.route = route

    settle_rule = controller.policy.lookup("route_rewrite.settle", "anonymous")
    refund_rule = controller.policy.lookup("route_rewrite.refund", "anonymous")

    async def call_refund():
        _, r = await app.asgi_client.get("/refund")
        return r.status

    # settle 席位空闲：改写后的 /refund 按 settle 计费一次并成功
    assert await call_refund() == 200
    assert controller.occupancy(settle_rule) == (0, 0)
    assert controller.occupancy(refund_rule) == (0, 0)

    # 占住 settle 席位：改写后的 /refund 必须被 settle 桶拒绝，
    # 而 refund 桶始终空闲（证明没有重复计费到 refund）
    busy = asyncio.ensure_future(app.asgi_client.get("/settle"))
    await asyncio.sleep(0.25)
    _, rejected = await app.asgi_client.get("/refund")
    assert rejected.status == 503
    assert rejected.headers["x-admission-version"] == controller.version
    assert controller.occupancy(refund_rule) == (0, 0)
    await busy


@pytest.mark.asyncio
async def test_queue_timeout_returns_503_and_frees_slot():
    app = Sanic("queue_timeout_it")

    @app.get("/settle")
    async def settle(request):
        await asyncio.sleep(1.0)
        return text("settled")

    controller = install_admission(
        app,
        [
            AdmissionRule(
                "queue_timeout_it.settle",
                1,
                queue_limit=1,
                queue_timeout=0.15,
            )
        ],
    )
    rule = controller.policy.lookup("queue_timeout_it.settle", "anonymous")

    async def call():
        _, r = await app.asgi_client.get("/settle")
        return r.status

    holder = asyncio.ensure_future(call())
    # 等 holder 真正占住席位
    for _ in range(50):
        await asyncio.sleep(0.02)
        if controller.occupancy(rule)[0] == 1:
            break
    queued = asyncio.ensure_future(call())
    # 确认第二个请求确实进入排队而非立即被受理
    for _ in range(50):
        await asyncio.sleep(0.02)
        if controller.occupancy(rule)[1] == 1:
            break
    assert controller.occupancy(rule) == (1, 1)
    # 排队超时（0.15s）远早于 holder 释放（1s），第二个请求应 503
    status = await asyncio.wait_for(queued, timeout=2.0)
    assert status == 503
    assert controller.occupancy(rule) == (1, 0)
    status_holder = await asyncio.wait_for(holder, timeout=2.0)
    assert status_holder == 200
    # 席位与排队名额全部回收，新请求可立即受理
    for _ in range(50):
        await asyncio.sleep(0.02)
        if controller.occupancy(rule) == (0, 0):
            break
    assert controller.occupancy(rule) == (0, 0)


@pytest.mark.asyncio
async def test_policy_switch_takes_effect_for_new_requests():
    app = build_app("policy_switch")
    controller = install_admission(
        app, [AdmissionRule("policy_switch.settle", 1, queue_limit=0)]
    )
    new_version = controller.update_policy(
        [AdmissionRule("policy_switch.settle", 5, queue_limit=0)],
        version="v-canary",
    )
    assert new_version == "v-canary"

    async def call():
        _, r = await app.asgi_client.get("/settle")
        return r

    # 容量扩大后多个并发请求都能受理
    responses = await asyncio.gather(
        *[app.asgi_client.get("/settle") for _ in range(3)]
    )
    for _, response in responses:
        assert response.status == 200
        assert response.headers["x-admission-version"] == "v-canary"


def test_controller_exposed_on_app_context():
    app = build_app("ctx_exposed")
    controller = install_admission(
        app, [AdmissionRule("ctx_exposed.settle", 1)]
    )
    assert app.ctx.admission is controller


# ---------------------------------------------------------------------- #
# 客户端断开（ASGI 任务取消，等价于连接丢失）
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_client_disconnect_releases_seats_while_queued_and_handling():
    app = Sanic("disconnect_asgi")
    gate = asyncio.Event()

    @app.get("/settle")
    async def settle(request):
        await gate.wait()
        return text("settled")

    controller = install_admission(
        app, [AdmissionRule("disconnect_asgi.settle", 1, queue_limit=1)]
    )
    rule = controller.policy.lookup("disconnect_asgi.settle", "anonymous")

    async def call_settle():
        _, _ = await app.asgi_client.get("/settle")

    loop = asyncio.get_event_loop()

    # 一个请求在处理中占住席位
    handling = loop.create_task(call_settle())
    for _ in range(50):
        await asyncio.sleep(0.02)
        if controller.occupancy(rule)[0] == 1:
            break
    assert controller.occupancy(rule) == (1, 0)

    # 第二个请求进入排队
    queued = loop.create_task(call_settle())
    for _ in range(50):
        await asyncio.sleep(0.02)
        if controller.occupancy(rule)[1] == 1:
            break
    assert controller.occupancy(rule) == (1, 1)

    # 排队客户端断开：排队名额必须归还
    queued.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await queued
    for _ in range(50):
        await asyncio.sleep(0.02)
        if controller.occupancy(rule)[1] == 0:
            break
    assert controller.occupancy(rule) == (1, 0)

    # 处理中客户端断开：占住的席位也必须归还
    handling.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handling
    gate.set()
    for _ in range(50):
        await asyncio.sleep(0.02)
        if controller.occupancy(rule)[0] == 0:
            break
    assert controller.occupancy(rule) == (0, 0)

    # 席位全部归还后，新请求可正常受理并完成
    _, response = await app.asgi_client.get("/settle")
    assert response.status == 200
    assert controller.occupancy(rule) == (0, 0)


@pytest.mark.asyncio
async def test_controller_wires_shutdown_and_reopen_lifecycle():
    app = build_app("shutdown_wiring")
    controller = install_admission(
        app, [AdmissionRule("shutdown_wiring.settle", 10)]
    )
    # 第一次请求：after_server_start 会 open，请求正常；请求结束后
    # ASGI 测试客户端触发 server.shutdown.before，协调器应进入停机
    _, r1 = await app.asgi_client.get("/settle")
    assert r1.status == 200
    assert controller.coordinator.shutting_down is True
    # 第二次请求：after_server_start 重新 open，服务恢复可用
    _, r2 = await app.asgi_client.get("/settle")
    assert r2.status == 200
    assert controller.coordinator.shutting_down is True


def test_shared_shutdown_broadcasts_to_all_workers():
    state = SharedState(slot_count=4, entry_capacity=8)
    rule = AdmissionRule("r.*", 1, queue_limit=2, name="r")
    policy = AdmissionPolicy.create([rule], version="v")
    key = bucket_key(rule)
    coords = [SharedCoordinator(state) for _ in range(3)]
    for coord in coords:
        coord.apply_policy(policy)

    async def scenario():
        coords[0].shutdown()
        # 所有 worker 都应观察到停机标记，新申请一律失败
        for coord in coords:
            assert coord.shutting_down is True
            token = AdmissionToken(key, "v", rule=rule)
            with pytest.raises(AdmissionUnavailable):
                await coord.wait_for_seat(
                    key, rule, token, monotonic=time.monotonic
                )
        # 重新开放后恢复受理
        coords[1].open()
        token = AdmissionToken(key, "v", rule=rule)
        await coords[2].wait_for_seat(
            key, rule, token, monotonic=time.monotonic
        )
        assert token.granted
        await coords[2].release_async(token)
        assert coords[0].snapshot(key) == (0, 0)

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_rejected_request_can_succeed_on_retry_after_release():
    app = Sanic("retry_after_release")

    @app.get("/settle")
    async def settle(request):
        await asyncio.sleep(0.15)
        return text("settled")

    controller = install_admission(
        app,
        [
            AdmissionRule(
                "retry_after_release.settle",
                1,
                queue_limit=0,
                retry_after=1,
            )
        ],
    )

    async def call():
        _, r = await app.asgi_client.get("/settle")
        return r

    first = asyncio.ensure_future(call())
    await asyncio.sleep(0.05)
    rejected = await call()
    assert rejected.status == 503
    assert rejected.headers["retry-after"] == "1"
    # 被拒不占用席位：首个请求正常完成后，同一调用方立即重试即成功
    await first
    retried = await call()
    assert retried.status == 200
    assert retried.headers["x-admission-version"] == controller.version
