from __future__ import annotations

import asyncio
import hashlib
import itertools

from asyncio import (
    AbstractEventLoop,
    CancelledError,
    Future,
    get_running_loop,
)
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from multiprocessing import Array, Condition, Value
from multiprocessing.sharedctypes import SynchronizedArray
from typing import TYPE_CHECKING, Callable, Optional, Union

from .exceptions import (
    AdmissionRejected,
    AdmissionTimeout,
    AdmissionUnavailable,
)
from .policy import AdmissionPolicy, AdmissionRule


if TYPE_CHECKING:
    from .controller import AdmissionController


# 排队条目状态
_EMPTY = 0
_WAITING = 1
_GRANTED = 2
_CANCELLED = 3


def bucket_key(rule: AdmissionRule) -> str:
    """席位桶身份：只由路由/调用方模式决定，与额度参数无关。

    策略切换前后，同一桶的在途席位连续计数，调整额度不会导致席位泄漏。
    """
    return ",".join(rule.route_patterns) + "|" + ",".join(rule.caller_patterns)


@dataclass(eq=False)
class AdmissionToken:
    """一次请求的准入凭据；release 幂等。"""

    key: Optional[str]
    version: str
    granted: bool = False
    queued: bool = False
    released: bool = False
    generation: int = 0
    slot_index: Optional[int] = None
    rule: Optional[AdmissionRule] = None
    controller: Optional["AdmissionController"] = field(
        default=None, repr=False, compare=False
    )

    def mark(self, *, granted: bool, queued: bool, generation: int) -> None:
        self.granted = granted
        self.queued = queued
        self.generation = generation

    def release(self) -> None:
        if self.controller is not None:
            self.controller.release(self)


class _Waiter:
    __slots__ = ("future", "priority", "seq", "deadline", "expired", "key")

    def __init__(
        self,
        future: Future,
        priority: int,
        seq: int,
        deadline: Optional[float],
        key: str,
    ) -> None:
        self.future = future
        self.priority = priority
        self.seq = seq
        self.deadline = deadline
        self.expired = False
        self.key = key

    def __lt__(self, other: "_Waiter") -> bool:
        # 高优先级先获得席位；同级先来先得，避免饥饿
        return (-self.priority, self.seq) < (-other.priority, other.seq)


class _LocalBucket:
    __slots__ = ("active", "capacity", "waiters")

    def __init__(self, capacity: int) -> None:
        self.active = 0
        self.capacity = capacity
        self.waiters: list[_Waiter] = []


class LocalCoordinator:
    """单 worker（单事件循环）席位协调器。

    纯本地数据结构，严格优先级 + FIFO；断连/超时/停机/换策略都能
    精确唤醒或失败对应的等待者。
    """

    def __init__(self, loop: Optional[AbstractEventLoop] = None) -> None:
        self._loop = loop
        self._buckets: dict[str, _LocalBucket] = {}
        self._seq = itertools.count()
        self._shutting_down = False
        self._generation = 0

    @property
    def loop(self) -> AbstractEventLoop:
        if self._loop is None:
            self._loop = get_running_loop()
        return self._loop

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def shutting_down(self) -> bool:
        return self._shutting_down

    def active(self, key: str) -> int:
        bucket = self._buckets.get(key)
        return bucket.active if bucket else 0

    def waiting(self, key: str) -> int:
        bucket = self._buckets.get(key)
        return len(bucket.waiters) if bucket else 0

    def apply_policy(self, policy: AdmissionPolicy) -> None:
        self._generation += 1
        caps = {bucket_key(rule): rule.concurrency for rule in policy.rules}
        for key, capacity in caps.items():
            bucket = self._buckets.get(key)
            if bucket is None:
                self._buckets[key] = _LocalBucket(capacity)
            else:
                bucket.capacity = capacity
        # 已消失的桶：容量置 0 并中断排队者，但保留在途计数，
        # 这样策略再次包含该桶时不会丢失席位计数而超额受理
        for key, bucket in self._buckets.items():
            if key not in caps:
                bucket.capacity = 0
                for waiter in list(bucket.waiters):
                    self._abort(waiter)
        for key in caps:
            self._pump(self._buckets[key])

    def shutdown(self) -> None:
        self._shutting_down = True
        for bucket in self._buckets.values():
            for waiter in list(bucket.waiters):
                self._abort(waiter)

    def open(self) -> None:
        """（重新）开放准入：服务启动或进程内重启时清除停机标记。"""
        self._shutting_down = False

    async def wait_for_seat(
        self,
        key: str,
        rule: AdmissionRule,
        token: AdmissionToken,
        *,
        monotonic: Callable[[], float],
    ) -> None:
        """占一个席位或排队；失败抛出准入异常。"""
        loop = self.loop
        if self._shutting_down:
            raise AdmissionUnavailable

        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _LocalBucket(rule.concurrency)
            self._buckets[key] = bucket

        # 已有等待者时必须排队，防止低优先级新请求插队
        if bucket.active < bucket.capacity and not bucket.waiters:
            bucket.active += 1
            token.mark(granted=True, queued=False, generation=self._generation)
            return

        if len(bucket.waiters) >= rule.queue_limit:
            raise AdmissionRejected(
                headers={"retry-after": str(rule.retry_after)}
            )

        future: Future = loop.create_future()
        timeout_handle = None
        waiter = _Waiter(future, rule.priority, next(self._seq), None, key)
        bucket.waiters.append(waiter)
        token.mark(granted=False, queued=True, generation=self._generation)

        if rule.queue_timeout is not None:
            # queue_timeout 是相对秒数，用事件循环自己的时钟调度，
            # 不与可能被替换的 monotonic 注入混用基准
            def _on_timeout() -> None:
                if waiter.expired or waiter.future.done():
                    return
                waiter.expired = True
                self._detach(bucket, waiter)
                if not waiter.future.cancelled():
                    waiter.future.set_exception(
                        AdmissionTimeout(headers={"retry-after": "0"})
                    )
                self._pump(bucket)

            timeout_handle = loop.call_later(rule.queue_timeout, _on_timeout)
        try:
            await future
            token.mark(granted=True, queued=False, generation=self._generation)
        except CancelledError:
            # 排队期间客户端断开（或停机/换策略中断）：让出排队名额
            self._detach(bucket, waiter)
            self._pump(bucket)
            raise
        finally:
            if timeout_handle is not None:
                timeout_handle.cancel()

    def release(self, token: AdmissionToken) -> None:
        if not token.granted or token.key is None:
            return
        bucket = self._buckets.get(token.key)
        if bucket is None:
            return
        bucket.active = max(0, bucket.active - 1)
        self._pump(bucket)

    async def release_async(self, token: AdmissionToken) -> None:
        self.release(token)

    def _pump(self, bucket: _LocalBucket) -> None:
        while bucket.waiters and bucket.active < bucket.capacity:
            waiter = min(bucket.waiters)
            if waiter.expired or waiter.future.done():
                bucket.waiters.remove(waiter)
                continue
            bucket.waiters.remove(waiter)
            if waiter.future.cancelled():
                continue
            # 等待者正式占用席位（release 已先减 1，这里加回）
            bucket.active += 1
            waiter.future.set_result(None)

    def _detach(self, bucket: _LocalBucket, waiter: _Waiter) -> None:
        try:
            bucket.waiters.remove(waiter)
        except ValueError:
            pass

    def _abort(self, waiter: _Waiter) -> None:
        if not waiter.future.done() and not waiter.future.cancelled():
            waiter.future.set_exception(AdmissionUnavailable)


class SharedState:
    """在主进程创建、经 shared_ctx 遗传给各 worker 的跨进程原语。

    每个槽位对应一个席位桶（策略装载时校验不冲突），槽内含：
    active/queued 计数、一张定长排序队列表和一个条件变量，
    所有判定在同一把锁内完成。
    """

    def __init__(self, slot_count: int, entry_capacity: int = 256) -> None:
        if slot_count <= 0:
            raise ValueError("slot_count 必须大于 0")
        if entry_capacity <= 0:
            raise ValueError("entry_capacity 必须大于 0")
        self.slot_count = slot_count
        self.entry_capacity = entry_capacity
        total = slot_count * entry_capacity
        self._prio: SynchronizedArray = Array("q", total)
        self._seq: SynchronizedArray = Array("Q", total)
        self._entry_state: SynchronizedArray = Array("B", total)
        self._active: SynchronizedArray = Array("I", slot_count)
        self._queued: SynchronizedArray = Array("I", slot_count)
        self._capacity: SynchronizedArray = Array("I", slot_count)
        self._queue_limit: SynchronizedArray = Array("I", slot_count)
        self._open: SynchronizedArray = Array("b", slot_count)
        self._ticket: SynchronizedArray = Array("Q", slot_count)
        self.conditions = tuple(Condition() for _ in range(slot_count))
        self.shutting_down = Value("b", False, lock=True)

    @staticmethod
    def slot_for(key: str, slot_count: int) -> int:
        digest = hashlib.sha1(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % slot_count

    def _base(self, slot: int) -> int:
        return slot * self.entry_capacity


class SharedCoordinator:
    """多 worker 共享席位协调器。

    对同一桶的“受理 / 排队 / 拒绝”判定在共享锁内串行完成，
    因此多个 worker 的本地判定汇集成确定结果：不会超额受理，
    也不会在有名额时拒绝；等待队列按优先级、同级按取票号（FIFO）
    全局排序。阻塞原语在线程池中执行，不卡住事件循环。
    """

    def __init__(
        self,
        state: SharedState,
        loop: Optional[AbstractEventLoop] = None,
    ) -> None:
        self._state = state
        self._loop = loop
        self._generation = 0
        # 每个排队者需要一个线程在共享条件变量上等待，池容量必须
        # 覆盖“所有桶排队上限之和”，否则会出现席位释放却无人被唤醒。
        self._executor: Optional[ThreadPoolExecutor] = None
        self._executor_size = 0

    def _ensure_executor(self, policy: AdmissionPolicy) -> ThreadPoolExecutor:
        # 每个桶额外预留若干线程用于 register/release/cancel 等短操作
        needed = (
            sum(rule.queue_limit for rule in policy.rules)
            + self._state.slot_count
            + 4
        )
        if self._executor is None or needed > self._executor_size:
            if self._executor is not None:
                self._executor.shutdown(wait=False)
            self._executor = ThreadPoolExecutor(
                max_workers=needed,
                thread_name_prefix="sanic-admission",
            )
            self._executor_size = needed
        return self._executor

    def close_executor(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None
            self._executor_size = 0

    @property
    def loop(self) -> AbstractEventLoop:
        if self._loop is None:
            self._loop = get_running_loop()
        return self._loop

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def shutting_down(self) -> bool:
        return bool(self._state.shutting_down.value)

    def apply_policy(self, policy: AdmissionPolicy) -> None:
        self._generation += 1
        caps: dict[int, int] = {}
        qlimits: dict[int, int] = {}
        seen: dict[int, str] = {}
        for rule in policy.rules:
            key = bucket_key(rule)
            slot = SharedState.slot_for(key, self._state.slot_count)
            previous = seen.get(slot)
            if previous is not None and previous != key:
                raise RuntimeError(
                    "准入桶槽位冲突，请增大 SharedState 的 slot_count"
                )
            seen[slot] = key
            caps[slot] = rule.concurrency
            qlimits[slot] = rule.queue_limit
            # 每张表既要容纳在途 GRANTED 墓碑（≤ concurrency），
            # 也要容纳 WAITING 排队条目（≤ queue_limit），故容量必须
            # 覆盖二者之和，否则高负载下会把本应排队的请求保守拒绝。
            if rule.concurrency + rule.queue_limit > (
                self._state.entry_capacity
            ):
                raise RuntimeError(
                    "concurrency + queue_limit 超过 SharedState.entry_capacity"
                )
        self._ensure_executor(policy)
        # 容量、排队上限与开关状态对所有 worker 共享，判定才会一致
        for slot, capacity in caps.items():
            with self._state.conditions[slot]:
                self._state._capacity[slot] = capacity
                self._state._queue_limit[slot] = qlimits[slot]
                self._state._open[slot] = 1
                self._state.conditions[slot].notify_all()
        for slot in range(self._state.slot_count):
            if slot not in caps:
                with self._state.conditions[slot]:
                    self._state._open[slot] = 0
                    self._state.conditions[slot].notify_all()

    def shutdown(self) -> None:
        with self._state.shutting_down.get_lock():
            self._state.shutting_down.value = True
        for condition in self._state.conditions:
            with condition:
                condition.notify_all()

    def open(self) -> None:
        """（重新）开放准入：服务启动或进程内重启时清除停机标记。"""
        with self._state.shutting_down.get_lock():
            self._state.shutting_down.value = False
        for condition in self._state.conditions:
            with condition:
                condition.notify_all()

    # ------------------------------------------------------------------ #
    # 以下方法在线程池中执行；除显式说明外均持槽位锁
    # ------------------------------------------------------------------ #

    def _register(self, slot: int, rule: AdmissionRule) -> Union[str, int]:
        condition = self._state.conditions[slot]
        with condition:
            if self._state.shutting_down.value:
                return "shutdown"
            if not self._state._open[slot]:
                return "closed"
            active = self._state._active[slot]
            queued = self._state._queued[slot]
            capacity = self._state._capacity[slot]
            queue_limit = self._state._queue_limit[slot]
            if active < capacity and queued == 0:
                self._state._active[slot] = active + 1
                return "granted"
            if queued >= queue_limit:
                return "rejected"
            base = self._state._base(slot)
            index = self._find_empty(slot)
            if index is None:
                # 共享表容量不足：保守拒绝，不超额排队
                return "rejected"
            ticket = self._state._ticket[slot]
            self._state._ticket[slot] = ticket + 1
            pos = base + index
            self._state._prio[pos] = rule.priority
            self._state._seq[pos] = ticket
            self._state._entry_state[pos] = _WAITING
            self._state._queued[slot] = queued + 1
            return index

    def _find_empty(self, slot: int) -> Optional[int]:
        base = self._state._base(slot)
        cap = self._state.entry_capacity
        for index in range(cap):
            if self._state._entry_state[base + index] == _EMPTY:
                return index
        return None

    def _best_waiting(self, slot: int) -> Optional[int]:
        """返回优先级最高、取票号最小的等待条目在槽内的下标。"""
        base = self._state._base(slot)
        cap = self._state.entry_capacity
        best: Optional[tuple[int, int, int]] = None
        for index in range(cap):
            pos = base + index
            if self._state._entry_state[pos] == _WAITING:
                candidate = (
                    self._state._prio[pos],
                    -self._state._seq[pos],
                    -index,
                )
                if best is None or candidate > best:
                    best = candidate
        if best is None:
            return None
        return -best[2]

    def _park(
        self,
        slot: int,
        index: int,
        deadline: Optional[float],
        monotonic: Callable[[], float],
    ) -> str:
        condition = self._state.conditions[slot]
        pos = self._state._base(slot) + index
        with condition:
            while True:
                state = self._state._entry_state[pos]
                if state == _CANCELLED:
                    # _cancel 已把 WAITING→CANCELLED 并减过排队计数，
                    # 这里只清空墓碑，不能再次减计数
                    self._state._entry_state[pos] = _EMPTY
                    return "cancelled"
                if self._state.shutting_down.value:
                    self._leave_queue(slot, pos)
                    return "shutdown"
                if not self._state._open[slot]:
                    # 策略切换后该席位桶已删除：中断排队，不占名额
                    self._leave_queue(slot, pos)
                    condition.notify_all()
                    return "closed"
                if deadline is not None and monotonic() >= deadline:
                    self._leave_queue(slot, pos)
                    condition.notify_all()
                    return "timeout"
                best = self._best_waiting(slot)
                active = self._state._active[slot]
                capacity = self._state._capacity[slot]
                if (
                    self._state._open[slot]
                    and best == index
                    and active < capacity
                ):
                    # 置 GRANTED 并保留墓碑到释放：席位生命周期与墓碑一致，
                    # 授予与取消之间不存在“无人认领”的窗口。
                    self._state._entry_state[pos] = _GRANTED
                    queued = self._state._queued[slot]
                    self._state._queued[slot] = max(0, queued - 1)
                    self._state._active[slot] = active + 1
                    condition.notify_all()
                    return "granted"
                remaining: Optional[float] = None
                if deadline is not None:
                    remaining = max(0.0, deadline - monotonic())
                condition.wait(remaining)

    def _leave_queue(self, slot: int, pos: int) -> None:
        """WAITING→EMPTY：离开排队队列并减一次排队计数。调用方持锁。"""
        if self._state._entry_state[pos] == _WAITING:
            self._state._entry_state[pos] = _EMPTY
            queued = self._state._queued[slot]
            self._state._queued[slot] = max(0, queued - 1)

    def _cancel(self, slot: int, index: int) -> None:
        """排队 await 被取消时触发；与正常授予路径恰好只有一个生效。"""
        condition = self._state.conditions[slot]
        pos = self._state._base(slot) + index
        with condition:
            state = self._state._entry_state[pos]
            if state == _WAITING:
                # 尚未授予：置取消墓碑，由 park 线程在循环顶部回收
                self._state._entry_state[pos] = _CANCELLED
                queued = self._state._queued[slot]
                self._state._queued[slot] = max(0, queued - 1)
                condition.notify_all()
            elif state == _GRANTED:
                # 授予已落地但 await 被取消：请求拿不到席位，直接归还
                self._state._entry_state[pos] = _EMPTY
                active = self._state._active[slot]
                self._state._active[slot] = max(0, active - 1)
                condition.notify_all()
            # EMPTY：park 已按 shutdown/closed/timeout 离场，无需处理

    def _release(self, slot: int, index: int) -> None:
        """正常结束：清 GRANTED 墓碑并归还席位（幂等）。"""
        condition = self._state.conditions[slot]
        pos = self._state._base(slot) + index
        with condition:
            if self._state._entry_state[pos] == _GRANTED:
                self._state._entry_state[pos] = _EMPTY
                active = self._state._active[slot]
                self._state._active[slot] = max(0, active - 1)
                condition.notify_all()

    def snapshot(self, key: str) -> tuple[int, int]:
        slot = SharedState.slot_for(key, self._state.slot_count)
        with self._state.conditions[slot]:
            return (
                self._state._active[slot],
                self._state._queued[slot],
            )

    async def wait_for_seat(
        self,
        key: str,
        rule: AdmissionRule,
        token: AdmissionToken,
        *,
        monotonic: Callable[[], float],
    ) -> None:
        loop = self.loop
        if self._executor is None:
            # 未经 apply_policy（如未启动直接调用）：用默认短任务池
            pool = None
        else:
            pool = self._executor
        slot = SharedState.slot_for(key, self._state.slot_count)
        deadline = (
            monotonic() + rule.queue_timeout
            if rule.queue_timeout is not None
            else None
        )

        outcome = await loop.run_in_executor(pool, self._register, slot, rule)
        if outcome == "granted":
            token.mark(granted=True, queued=False, generation=self._generation)
            return
        if outcome == "rejected":
            raise AdmissionRejected(
                headers={"retry-after": str(rule.retry_after)}
            )
        if outcome == "shutdown" or outcome == "closed":
            raise AdmissionUnavailable

        index = int(outcome)
        token.slot_index = index
        token.mark(granted=False, queued=True, generation=self._generation)
        park_future = loop.run_in_executor(
            pool,
            self._park,
            slot,
            index,
            deadline,
            monotonic,
        )
        try:
            result = await park_future
        except CancelledError:
            # 排队期间或授予瞬间客户端断开：不 await（取消点 await 会立刻
            # 再次抛出），触发即忘地交给 _cancel 处理排队名额或刚授予席位
            if pool is not None:
                pool.submit(self._cancel, slot, index)
            else:
                loop.run_in_executor(None, self._cancel, slot, index)
            raise

        if result == "granted":
            # 保留 slot_index：墓碑要保留到请求结束，由 _release(slot,index)
            # 回收；授予后再被取消则由 _cancel 看到 GRANTED 直接归还
            token.mark(granted=True, queued=False, generation=self._generation)
            return
        if result == "shutdown" or result == "closed":
            raise AdmissionUnavailable
        if result == "cancelled":
            raise CancelledError
        raise AdmissionTimeout(headers={"retry-after": "0"})

    def _release_active(self, slot: int) -> None:
        """释放一个无墓碑席位（直接受理路径，从未排队）。幂等由令牌保证。"""
        condition = self._state.conditions[slot]
        with condition:
            active = self._state._active[slot]
            self._state._active[slot] = max(0, active - 1)
            condition.notify_all()

    def release(self, token: AdmissionToken) -> None:
        if not token.granted or token.key is None:
            return
        slot = SharedState.slot_for(token.key, self._state.slot_count)
        index = token.slot_index
        pool = self._executor
        if pool is not None:
            # 不阻塞事件循环：mp 条件锁可能被其他进程短暂持有
            if index is None:
                pool.submit(self._release_active, slot)
            else:
                pool.submit(self._release, slot, index)
        else:  # pragma: no cover - apply_policy 之前不会有已授予令牌
            self._release_active(slot)

    async def release_async(self, token: AdmissionToken) -> None:
        """正常请求结束路径：等待释放真正落地，保证归还确定性。"""
        if not token.granted or token.key is None:
            return
        slot = SharedState.slot_for(token.key, self._state.slot_count)
        index = token.slot_index
        pool = self._executor
        if pool is None:  # pragma: no cover
            self._release_active(slot)
            return
        if index is None:
            future = pool.submit(self._release_active, slot)
        else:
            future = pool.submit(self._release, slot, index)
        await asyncio.wrap_future(future)

    def release_threadsafe(self, token: AdmissionToken) -> None:
        self.release(token)
