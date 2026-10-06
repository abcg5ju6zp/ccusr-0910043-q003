from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from hashlib import sha1
from typing import Iterable, Union


RoutePattern = str
CallerPattern = str


def _patterns(value: Union[str, Iterable[str]]) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _specificity(pattern: str) -> tuple[int, int]:
    """越大越具体：精确匹配 > 前缀通配 > 其他通配；长 pattern 优先。"""
    if not any(ch in pattern for ch in "*?[]"):
        return (3, len(pattern))
    if pattern.endswith(".*") and not any(ch in pattern[:-2] for ch in "*?[]"):
        return (2, len(pattern))
    return (1, len(pattern))


@dataclass(frozen=True)
class AdmissionRule:
    """一条准入规则：路由 × 调用方 → 席位 / 排队 / 优先级。

    Args:
        routes: 路由名 glob，可多个；``"*"`` 匹配全部。
        callers: 调用方标识 glob，默认 ``"*"``。
        concurrency: 该席位桶允许同时执行的请求数（必填，>0）。
        queue_limit: 席位占满后允许排队的请求数；0 表示立即拒绝。
        priority: 排队时的调度优先级，数值越大越先获得席位。
        queue_timeout: 排队最长秒数；``None`` 表示仅受连接断开约束。
        retry_after: 被拒响应中 Retry-After 的建议秒数。
    """

    routes: Union[str, tuple[str, ...]]
    concurrency: int
    callers: Union[str, tuple[str, ...]] = "*"
    queue_limit: int = 0
    priority: int = 0
    queue_timeout: Union[float, None] = None
    retry_after: int = 1
    name: str = ""
    _index: int = field(default=0, compare=False, repr=False)

    def __post_init__(self) -> None:
        if not self.routes:
            raise ValueError("AdmissionRule.routes 不能为空")
        if not self.callers:
            raise ValueError("AdmissionRule.callers 不能为空")
        if self.concurrency <= 0:
            raise ValueError("AdmissionRule.concurrency 必须大于 0")
        if self.queue_limit < 0:
            raise ValueError("AdmissionRule.queue_limit 不能为负")
        if self.queue_timeout is not None and self.queue_timeout <= 0:
            raise ValueError("AdmissionRule.queue_timeout 必须大于 0")

    @property
    def route_patterns(self) -> tuple[str, ...]:
        return _patterns(self.routes)

    @property
    def caller_patterns(self) -> tuple[str, ...]:
        return _patterns(self.callers)

    def matches(
        self, route_name: str, caller: str
    ) -> Union[tuple[int, int, int, int], None]:
        """返回匹配评分；不匹配返回 None。"""
        route_score = (0, 0)
        for pattern in self.route_patterns:
            if fnmatch(route_name, pattern):
                score = _specificity(pattern)
                if score > route_score:
                    route_score = score
        if route_score == (0, 0):
            return None

        caller_score = (0, 0)
        for pattern in self.caller_patterns:
            if fnmatch(caller, pattern):
                score = _specificity(pattern)
                if score > caller_score:
                    caller_score = score
        if caller_score == (0, 0):
            return None

        return (*caller_score, *route_score, -self._index)


@dataclass(frozen=True)
class AdmissionPolicy:
    """准入策略快照。

    规则集合一旦生成即不可变；切换策略通过替换整个快照完成，
    因此每个请求只可能按一个版本、一个席位桶计费。
    """

    rules: tuple[AdmissionRule, ...]
    version: str

    @classmethod
    def create(
        cls,
        rules: Iterable[AdmissionRule],
        *,
        version: Union[str, None] = None,
    ) -> "AdmissionPolicy":
        indexed = tuple(
            (
                rule
                if rule._index
                else AdmissionRule(
                    routes=rule.routes,
                    concurrency=rule.concurrency,
                    callers=rule.callers,
                    queue_limit=rule.queue_limit,
                    priority=rule.priority,
                    queue_timeout=rule.queue_timeout,
                    retry_after=rule.retry_after,
                    name=rule.name,
                    _index=i,
                )
            )
            for i, rule in enumerate(rules)
        )
        if version is None:
            material = "|".join(
                repr(
                    (
                        sorted(rule.route_patterns),
                        sorted(rule.caller_patterns),
                        rule.concurrency,
                        rule.queue_limit,
                        rule.priority,
                        rule.queue_timeout,
                    )
                )
                for rule in indexed
            )
            version = sha1(material.encode()).hexdigest()[:12]
        return cls(rules=indexed, version=version)

    def lookup(
        self, route_name: str, caller: str
    ) -> Union[AdmissionRule, None]:
        """选出最具体的唯一规则；无规则命中时返回 None（不做准入）。"""
        best: Union[tuple, None] = None
        winner: Union[AdmissionRule, None] = None
        for rule in self.rules:
            score = rule.matches(route_name, caller)
            if score is not None and (best is None or score > best):
                best = score
                winner = rule
        return winner
