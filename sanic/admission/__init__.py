"""按路由与调用方的准入控制：席位、排队、优先级与策略版本。"""

from __future__ import annotations

from .controller import (
    DEFAULT_CALLER_HEADER,
    VERSION_HEADER,
    AdmissionController,
    install_admission,
    setup_shared_admission,
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


__all__ = (
    "AdmissionController",
    "AdmissionPolicy",
    "AdmissionRejected",
    "AdmissionRule",
    "AdmissionTimeout",
    "AdmissionToken",
    "AdmissionUnavailable",
    "DEFAULT_CALLER_HEADER",
    "LocalCoordinator",
    "SharedCoordinator",
    "SharedState",
    "VERSION_HEADER",
    "bucket_key",
    "install_admission",
    "setup_shared_admission",
)
