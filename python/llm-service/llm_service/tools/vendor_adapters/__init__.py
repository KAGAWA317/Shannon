"""Vendor adapters for domain-specific API transformations.
面向特定厂商 API 转换的适配器。

This module provides adapter registries for vendor-specific tool integrations.
本模块提供用于厂商特定 tool 集成的适配器注册表。

get_vendor_adapter() - For OpenAPI-based tools
  - Purpose: Transform request/response bodies for OpenAPI specs
  - Security: Whitelist-based (must manually add vendors to ALLOWED_VENDORS)
  - Usage: OpenAPITool uses these adapters to modify API calls
get_vendor_adapter()——用于基于 OpenAPI 的 tool
  - 用途：为 OpenAPI 规范转换 request/response body
  - 安全：基于白名单（必须手动将厂商加入 ALLOWED_VENDORS）
  - 用法：OpenAPITool 使用这些适配器修改 API 调用

ADDING NEW ADAPTERS:
新增适配器：

For OpenAPI tools:
  1. Create vendor_adapters/myvendor/adapter.py with MyVendorAdapter class
  2. Add "myvendor" to ALLOWED_VENDORS whitelist (see get_vendor_adapter)
  3. Add explicit import in get_vendor_adapter() function
对于 OpenAPI tool：
  1. 创建 vendor_adapters/myvendor/adapter.py，其中包含 MyVendorAdapter 类
  2. 将 "myvendor" 加入 ALLOWED_VENDORS 白名单（参见 get_vendor_adapter）
  3. 在 get_vendor_adapter() 函数中添加显式 import

See docs/vendor-adapters.md for complete guide.
完整指南参见 docs/vendor-adapters.md。
"""


def get_vendor_adapter(name: str):
    """Return a vendor adapter for OpenAPI-based tools.
    返回用于基于 OpenAPI 的 tool 的厂商适配器。

    Uses whitelist-based security - vendors must be explicitly registered.
    采用白名单安全机制——厂商必须显式注册。

    Args:
        name: Vendor identifier
            厂商标识符

    Returns:
        Vendor adapter instance or None if vendor not found/not whitelisted
        厂商适配器实例；若厂商不存在或不在白名单中则返回 None
    """
    if not name:
        return None

    if ".." in name or "/" in name or "\\" in name:
        return None

    if not name.replace("_", "").replace("-", "").isalnum():
        return None

    vendor_name = name.lower()

    ALLOWED_VENDORS = {
        # Add your vendor names here as you implement them
        # 实现厂商适配器后，在此添加厂商名称
        # "myvendor",
    }

    if vendor_name not in ALLOWED_VENDORS:
        return None

    try:
        # Example vendor adapter registration:
        # 厂商适配器注册示例：
        # if vendor_name == "myvendor":
        #     from .myvendor.adapter import MyVendorAdapter
        #     return MyVendorAdapter()
        pass

    except ImportError:
        return None
    except Exception:
        return None

    return None
