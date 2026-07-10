from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path
from types import ModuleType
from typing import Any


_MODULE_LOCK = threading.Lock()
_MONITOR_MODULE: ModuleType | None = None
_INITIALISED_SIGNATURE: tuple[int, int, int] | None = None


def _option_simulator_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "option_simulator"


def _load_module() -> ModuleType:
    global _MONITOR_MODULE
    with _MODULE_LOCK:
        if _MONITOR_MODULE is not None:
            return _MONITOR_MODULE

        base_dir = _option_simulator_dir()
        module_path = base_dir / "strategy_monitor.py"
        if not module_path.exists():
            raise FileNotFoundError(f"strategy_monitor.py not found at {module_path}")

        base_dir_str = str(base_dir)
        if base_dir_str not in sys.path:
            sys.path.insert(0, base_dir_str)

        spec = importlib.util.spec_from_file_location(
            "option_simulator_strategy_monitor_bridge",
            module_path,
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load spec for {module_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        _MONITOR_MODULE = module
        return module


async def ensure_initialised(kite: Any, strategy_col: Any, db: Any) -> ModuleType:
    global _INITIALISED_SIGNATURE
    module = _load_module()
    signature = (id(kite), id(strategy_col), id(db))
    if _INITIALISED_SIGNATURE != signature:
        await module.start_monitor(kite, strategy_col, db=db)
        _INITIALISED_SIGNATURE = signature
    return module


async def start(kite: Any, strategy_col: Any, db: Any) -> dict:
    module = await ensure_initialised(kite, strategy_col, db)
    return await module.monitor_start_endpoint()


async def stop(kite: Any, strategy_col: Any, db: Any) -> dict:
    module = await ensure_initialised(kite, strategy_col, db)
    return await module.monitor_stop_endpoint()


async def stop_get(kite: Any, strategy_col: Any, db: Any) -> dict:
    module = await ensure_initialised(kite, strategy_col, db)
    return await module.monitor_stop_endpoint_get()


async def status(kite: Any, strategy_col: Any, db: Any) -> dict:
    module = await ensure_initialised(kite, strategy_col, db)
    return module.monitor_status()


async def reentry_status(kite: Any, strategy_col: Any, db: Any) -> dict:
    module = await ensure_initialised(kite, strategy_col, db)
    return module.monitor_reentry_status()
