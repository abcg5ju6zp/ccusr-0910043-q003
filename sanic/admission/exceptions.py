"""准入控制子系统对外异常。"""

from sanic.exceptions import (
    AdmissionRejected,
    AdmissionTimeout,
    AdmissionUnavailable,
)


__all__ = (
    "AdmissionRejected",
    "AdmissionTimeout",
    "AdmissionUnavailable",
)
