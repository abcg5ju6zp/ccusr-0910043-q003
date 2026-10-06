# Sanic 服务框架

本项目提供异步 HTTP 服务、路由、蓝图、中间件、信号、后台任务和工作进程管理能力。生产源码位于 `sanic/`，核心回归测试位于 `tests/`。

## 安装

`python3 -m pip install --break-system-packages --no-build-isolation -e '.[test]'`

## 测试

`python3 -m pytest -q`

## 构建

`python3 -m compileall -q sanic`

`python3 -m build --wheel --no-isolation`

## 使用

应用通过 `Sanic` 创建服务，可使用本地测试客户端验证请求、响应和生命周期行为。

## 准入控制

按路由与调用方配置并发席位、排队上限与优先级的准入子系统位于
`sanic/admission/`，支持本地协调器与多 worker 共享内存协调器。
设计、生命周期语义、多 worker 一致性与响应契约见
[准入控制基线](docs/admission.md)，回归测试见
`tests/test_admission.py`（`python3 -m pytest tests/test_admission.py -q`）。
