"""业务路由汇总：具体路由优先注册，避免被通用 /{module}/{entry_id} 动态路径抢先匹配。"""
from __future__ import annotations

from app.routers import maxwell_migration, resource

ROUTERS = [maxwell_migration, resource]
