from __future__ import annotations

import asyncio
import weakref

from time import monotonic
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Optional,
    Union,
)

from .exceptions import (
    AdmissionRejected,
    AdmissionTimeout,
    AdmissionUnavailable,
)
from .gates import (
    AdmissionToken,
    LocalCoordinator,
    SharedCoordinator,
    SharedState,
    bucket_key,
)
from .policy import AdmissionPolicy, AdmissionRule


if TYPE_CHECKING:
    from sanic.request.types import Request
    from sanic.response.types import BaseHTTPResponse

CallerResolver = Callable[["Request"], str]

VERSION_HEADER = "x-admission-version"
DEFAULT_CALLER_HEADER = "x-caller"
DEFAULT_ANONYMOUS_CALLER = "anonymous"


class AdmissionController:
    """准入控制器：策略、调用方识别与席位生命周期。

    - 在 ``http.handler.before``（请求中间件全部执行之后、处理器执行之前）
      取得最终路由再申请席位，因此中间件改写路由后只会按最终路由计费一次。
    - 席位凭据挂在执行任务的 done 回调上：正常完成、异常、响应超时、
      客户端断开、停机取消任务都会归还；排队中的取消由协调器直接清理。
    - ``http.lifecycle.response`` 写入本次准入版本头；拒绝响应同样携带，
      但不含任何其他调用方/租户的负载信息。
    """

    def __init__(
        self,
        rules: Iterable[AdmissionRule] = (),
        *,
        version: Optional[str] = None,
        caller_header: str = DEFAULT_CALLER_HEADER,
        caller_resolver: Optional[CallerResolver] = None,
        shared_state: Optional[SharedState] = None,
    ) -> None:
        self._policy = AdmissionPolicy.create(rules, version=version)
        self._caller_header = caller_header
        self._caller_resolver = caller_resolver
        self._shared_state = shared_state
        self._coordinator: Optional[
            Union[LocalCoordinator, SharedCoordinator]
        ] = None
        self._app: Any = None
        self._installed = False
        # 令牌的兜底登记：防止极端情况下任务对象被提前回收
        self._live: weakref.WeakSet[AdmissionToken] = weakref.WeakSet()

    # ------------------------------------------------------------------ #
    # 安装与策略
    # ------------------------------------------------------------------ #

    @property
    def policy(self) -> AdmissionPolicy:
        return self._policy

    @property
    def version(self) -> str:
        return self._policy.version

    @property
    def coordinator(
        self,
    ) -> Union[LocalCoordinator, SharedCoordinator]:
        if self._coordinator is None:
            self._coordinator = self._build_coordinator()
        return self._coordinator

    def _build_coordinator(
        self,
    ) -> Union[LocalCoordinator, SharedCoordinator]:
        if self._shared_state is not None:
            coordinator = SharedCoordinator(self._shared_state)
            coordinator.apply_policy(self._policy)
            return coordinator
        return LocalCoordinator()

    def use_shared_state(self, state: SharedState) -> None:
        """切换为多 worker 共享协调器（通常在 worker 启动监听器中调用）。"""
        self._shared_state = state
        self._coordinator = SharedCoordinator(state)
        self._coordinator.apply_policy(self._policy)

    def install(self, app: Any) -> "AdmissionController":
        if self._installed:
            return self
        self._app = app
        app.add_signal(self._on_before_handler, "http.handler.before")
        app.add_signal(self._on_response, "http.lifecycle.response")
        app.add_signal(self._on_exception, "http.lifecycle.exception")
        app.register_listener(self._after_server_start, "after_server_start")
        app.register_listener(self._before_server_stop, "before_server_stop")
        app.ctx.admission = self
        self._installed = True
        return self

    def update_policy(
        self,
        rules: Iterable[AdmissionRule],
        *,
        version: Optional[str] = None,
    ) -> str:
        """原子替换策略。

        新请求按新版本判定；被删除席位桶中的排队者立即失败并释放名额，
        在途请求继续按取得席位时的规则执行，结束后归还到同一桶。
        """
        self._policy = AdmissionPolicy.create(rules, version=version)
        self.coordinator.apply_policy(self._policy)
        return self._policy.version

    # ------------------------------------------------------------------ #
    # 调用方识别
    # ------------------------------------------------------------------ #

    def identify_caller(self, request: "Request") -> str:
        if self._caller_resolver is not None:
            try:
                caller = self._caller_resolver(request)
            except Exception:
                caller = None
            return caller or DEFAULT_ANONYMOUS_CALLER
        caller = request.headers.getone(self._caller_header, None)
        return caller or DEFAULT_ANONYMOUS_CALLER

    # ------------------------------------------------------------------ #
    # 准入与释放
    # ------------------------------------------------------------------ #

    def resolve(self, request: "Request") -> Optional[AdmissionRule]:
        route = request.route
        if route is None:
            return None
        return self._policy.lookup(route.name, self.identify_caller(request))

    async def admit(self, request: "Request") -> AdmissionToken:
        rule = self.resolve(request)
        version = self._policy.version
        # 无论是否受理，都记录本次判定版本，供响应头使用
        request.ctx.admission_version = version
        if rule is None:
            # 无规则命中（如健康检查）：不消耗任何席位
            token = AdmissionToken(
                key=None, version=version, rule=None, controller=self
            )
            return token

        coordinator = self.coordinator
        key = bucket_key(rule)
        token = AdmissionToken(
            key=key, version=version, rule=rule, controller=self
        )
        try:
            await coordinator.wait_for_seat(
                key, rule, token, monotonic=monotonic
            )
        except (
            AdmissionRejected,
            AdmissionTimeout,
            AdmissionUnavailable,
        ) as exc:
            # 拒绝/排队超时/停机响应也要说明本次判定所用版本
            exc.headers = {
                **getattr(exc, "headers", {}),
                VERSION_HEADER: version,
            }
            raise
        return token

    def release(self, token: AdmissionToken) -> None:
        if token.released:
            return
        token.released = True
        if token.granted and token.key is not None and self._coordinator:
            self._coordinator.release(token)

    async def release_async(self, token: AdmissionToken) -> None:
        if token.released:
            return
        token.released = True
        if token.granted and token.key is not None and self._coordinator:
            await self._coordinator.release_async(token)

    # ------------------------------------------------------------------ #
    # Sanic 信号 / 监听器
    # ------------------------------------------------------------------ #

    async def _on_before_handler(self, request: "Request") -> None:
        token = await self.admit(request)
        request.ctx.admission_token = token
        if token.granted:
            current = asyncio.current_task()
            if current is not None:
                # 兜底网：传输已关闭等场景下 exception/response 信号都不会
                # 触发，任务被取消/结束时仍必须归还席位。release 幂等。
                current.add_done_callback(lambda _t: self.release(token))
            self._live.add(token)

    async def _on_response(
        self, request: "Request", response: "BaseHTTPResponse"
    ) -> None:
        version = getattr(request.ctx, "admission_version", None)
        if version is not None:
            try:
                response.headers[VERSION_HEADER] = version
            except Exception:  # pragma: no cover - 响应头不可写时忽略
                pass
        token = getattr(request.ctx, "admission_token", None)
        if token is not None:
            # 响应（含错误响应）已生成，处理器与中间件均已结束
            await self.release_async(token)

    async def _on_exception(
        self, request: "Request", exception: BaseException
    ) -> None:
        token = getattr(request.ctx, "admission_token", None)
        if token is not None:
            # 响应已无法发送（断连后处理失败、响应阶段异常等）时仍要归还
            await self.release_async(token)

    async def _after_server_start(self, app: Any, loop: Any = None) -> None:
        # 多 worker：主进程放入 shared_ctx 的共享原语经 fork/继承到达此处，
        # 在应用策略前绑定，避免先建本地协调器再丢弃
        if self._coordinator is None and self._shared_state is None:
            state = getattr(app.shared_ctx, "admission_state", None)
            if isinstance(state, SharedState):
                self.use_shared_state(state)
        # 清除可能存在的停机标记（进程内重启、测试客户端每请求触发
        # shutdown 生命周期），再按当前策略开放
        self.coordinator.open()
        self.coordinator.apply_policy(self._policy)

    async def _before_server_stop(self, app: Any, loop: Any = None) -> None:
        # 停机：新申请立即失败，排队者立即被唤醒失败；在途席位继续排空
        self.coordinator.shutdown()

    # ------------------------------------------------------------------ #
    # 可观测（仅本视角数据，不按调用方暴露他人负载）
    # ------------------------------------------------------------------ #

    def occupancy(self, rule: AdmissionRule) -> tuple[int, int]:
        key = bucket_key(rule)
        coordinator = self.coordinator
        if isinstance(coordinator, SharedCoordinator):
            return coordinator.snapshot(key)
        return coordinator.active(key), coordinator.waiting(key)


def install_admission(
    app: Any,
    rules: Iterable[AdmissionRule] = (),
    *,
    version: Optional[str] = None,
    caller_header: str = DEFAULT_CALLER_HEADER,
    caller_resolver: Optional[CallerResolver] = None,
    shared_state: Optional[SharedState] = None,
) -> AdmissionController:
    """创建并安装准入控制器。"""
    controller = AdmissionController(
        rules,
        version=version,
        caller_header=caller_header,
        caller_resolver=caller_resolver,
        shared_state=shared_state,
    )
    controller.install(app)
    return controller


def setup_shared_admission(
    app: Any,
    rules: Iterable[AdmissionRule] = (),
    *,
    slot_count: int = 64,
    entry_capacity: int = 256,
    version: Optional[str] = None,
    caller_header: str = DEFAULT_CALLER_HEADER,
    caller_resolver: Optional[CallerResolver] = None,
) -> AdmissionController:
    """多 worker 部署入口。

    主进程创建共享原语并放入 ``shared_ctx``（fork/继承给各 worker），
    每个 worker 启动后绑定到同一个共享协调器。
    """
    controller = AdmissionController(
        rules,
        version=version,
        caller_header=caller_header,
        caller_resolver=caller_resolver,
    )
    controller.install(app)

    def _create_state(_app: Any, _loop: Any = None) -> None:
        state = SharedState(
            slot_count=slot_count, entry_capacity=entry_capacity
        )
        # SharedState 内部全部是 multiprocessing 共享原语（Array/Condition/
        # Value），可安全跨 fork 继承；直接写入 __dict__ 以绕过
        # SharedContext 对自定义类型的“非安全对象”告警，避免误导运维。
        _app.shared_ctx.__dict__["admission_state"] = state

    # worker 的 after_server_start 监听器（控制器自身注册）会自动从
    # shared_ctx 绑定该共享原语，无需额外监听
    app.register_listener(_create_state, "main_process_start")
    return controller
