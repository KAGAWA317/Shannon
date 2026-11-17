"""
Tool Registry - Dynamic tool discovery and management
tool registry——动态 tool 发现与管理
"""

import importlib
import inspect
import logging
import os
from pathlib import Path

from .base import Tool, ToolMetadata

logger = logging.getLogger(__name__)


class ToolRegistry:
    """
    Central registry for all available tools.
    Supports dynamic loading and discovery.
    所有可用 tool 的中央 registry。
    支持动态加载与发现。
    """

    def __init__(self):
        self._tools: dict[str, type[Tool]] = {}
        self._instances: dict[str, Tool] = {}
        self._categories: dict[str, list[str]] = {}

    def register(self, tool_class: type[Tool], override: bool = False) -> None:
        """
        Register a tool class
        注册 tool 类

        Args:
            tool_class: The Tool class to register
                要注册的 Tool 类
            override: Whether to override existing tool with same name
                是否覆盖同名 tool
        """
        if not issubclass(tool_class, Tool):
            raise TypeError(f"{tool_class} must be a subclass of Tool")

        # Create temporary instance to get metadata
        # 创建临时实例以获取 metadata
        temp_instance = tool_class()
        metadata = temp_instance.metadata

        if metadata.name in self._tools and not override:
            raise ValueError(f"Tool '{metadata.name}' is already registered")

        self._tools[metadata.name] = tool_class

        # Update category index
        # 更新分类索引
        if metadata.category not in self._categories:
            self._categories[metadata.category] = []
        if metadata.name not in self._categories[metadata.category]:
            self._categories[metadata.category].append(metadata.name)

        logger.info(f"Registered tool: {metadata.name} (category: {metadata.category})")

    def unregister(self, tool_name: str) -> None:
        """Unregister a tool
        注销 tool"""
        if tool_name in self._tools:
            tool_class = self._tools[tool_name]
            temp_instance = tool_class()
            category = temp_instance.metadata.category

            del self._tools[tool_name]
            if tool_name in self._instances:
                del self._instances[tool_name]

            # Update category index
            # 更新分类索引
            if category in self._categories:
                self._categories[category].remove(tool_name)
                if not self._categories[category]:
                    del self._categories[category]

            logger.info(f"Unregistered tool: {tool_name}")

    def get_tool(self, name: str) -> Tool | None:
        """
        Get a tool instance by name.
        Uses singleton pattern - returns same instance for same tool.
        按名称获取 tool 实例。
        使用 singleton pattern——同一 tool 返回同一实例。
        """
        if name not in self._tools:
            return None

        if name not in self._instances:
            self._instances[name] = self._tools[name]()

        return self._instances[name]

    def list_tools(self) -> list[str]:
        """List all registered tool names"""
        return list(self._tools.keys())

    def list_categories(self) -> list[str]:
        """List all tool categories"""
        return list(self._categories.keys())

    def list_tools_by_category(self, category: str) -> list[str]:
        """List tools in a specific category"""
        return self._categories.get(category, [])

    def get_tool_metadata(self, name: str) -> ToolMetadata | None:
        """Get metadata for a tool"""
        tool = self.get_tool(name)
        return tool.metadata if tool else None

    def get_tool_schema(self, name: str) -> dict | None:
        """Get JSON schema for a tool"""
        tool = self.get_tool(name)
        return tool.get_schema() if tool else None

    def get_all_schemas(self) -> list[dict]:
        """Get schemas for all registered tools"""
        schemas = []
        for name in self._tools:
            tool = self.get_tool(name)
            if tool:
                schemas.append(tool.get_schema())
        return schemas

    def discover_tools(self, package_path: str) -> int:
        """
        Dynamically discover and register tools from a package.
        从包中动态发现并注册 tool。

        Args:
            package_path: Path to package containing tool modules
                包含 tool 模块的包路径

        Returns:
            Number of tools discovered and registered
            发现并注册的 tool 数量
        """
        discovered_count = 0

        # Convert to Path object
        # 转换为 Path 对象
        path = Path(package_path)

        if not path.exists():
            logger.warning(f"Tool package path does not exist: {package_path}")
            return 0

        # Find all Python files
        # 查找所有 Python 文件
        for py_file in path.rglob("*.py"):
            if py_file.name.startswith("_"):
                continue

            # Convert file path to module name
            # 将文件路径转换为模块名
            relative_path = py_file.relative_to(path.parent)
            module_name = str(relative_path.with_suffix("")).replace(os.sep, ".")

            try:
                # Import the module
                # 导入模块
                module = importlib.import_module(module_name)

                # Find all Tool subclasses in the module
                # 查找模块中的所有 Tool 子类
                for name, obj in inspect.getmembers(module):
                    if (
                        inspect.isclass(obj)
                        and issubclass(obj, Tool)
                        and obj is not Tool
                    ):
                        try:
                            self.register(obj)
                            discovered_count += 1
                        except ValueError as e:
                            # Tool might already be registered
                            # tool 可能已注册
                            logger.debug(f"Could not register {name}: {e}")
                        except Exception as e:
                            logger.error(f"Error registering {name}: {e}")

            except ImportError as e:
                logger.error(f"Could not import module {module_name}: {e}")
            except Exception as e:
                logger.error(f"Error processing module {module_name}: {e}")

        logger.info(f"Discovered {discovered_count} tools from {package_path}")
        return discovered_count

    def filter_tools_for_agent(
        self,
        categories: list[str] | None = None,
        exclude_dangerous: bool = True,
        max_cost: float | None = None,
    ) -> list[str]:
        """
        Filter tools based on agent requirements.
        按 agent 需求过滤 tool。

        Args:
            categories: Only include tools from these categories
                仅包含这些分类中的 tool
            exclude_dangerous: Whether to exclude dangerous tools
                是否排除危险 tool
            max_cost: Maximum cost per use
                单次使用最高成本

        Returns:
            List of tool names that match criteria
            符合条件 tool 的名称列表
        """
        filtered = []

        for name, tool_class in self._tools.items():
            tool = self.get_tool(name)
            if not tool:
                continue

            metadata = tool.metadata

            # Category filter
            # 分类过滤
            if categories and metadata.category not in categories:
                continue

            # Danger filter
            # 危险 tool 过滤
            if exclude_dangerous and metadata.dangerous:
                continue

            # Cost filter
            # 成本过滤
            if max_cost is not None and metadata.cost_per_use > max_cost:
                continue

            filtered.append(name)

        return filtered

    def filter_tools_by_task_type(
        self,
        task_type: str,
        allowed_tools: list[str] | None = None,
        max_tools: int = 5,
    ) -> list[str]:
        """
        Filter and limit tools based on task type for better LLM tool selection.
        按 task 类型过滤并限制 tool 数量，以便 LLM 更好地选择 tool。

        This prevents overwhelming the LLM with too many tools, which can cause
        choice paralysis and incorrect tool selection.
        这可避免过多 tool 让 LLM 不堪重负，从而导致选择困难或选错 tool。

        Args:
            task_type: Type of task (research, coding, analysis, browser, general)
                task 类型（research、coding、analysis、browser、general）
            allowed_tools: Base list of allowed tools to filter from (None = all tools)
                用于过滤的允许 tool 基础列表（None 表示全部 tool）
            max_tools: Maximum number of tools to return (default 5)
                返回 tool 数量上限（默认 5）

        Returns:
            List of tool names most relevant to the task type, limited to max_tools
            与 task 类型最相关的 tool 名称列表，数量不超过 max_tools
        """
        # Define task type to category mappings with priority order
        # 定义 task 类型到分类的映射，按优先级排序
        TASK_CATEGORY_MAP = {
            "research": ["search", "retrieval"],
            "coding": ["code", "file"],
            "analysis": ["calculation", "code"],
            "browser": ["browser"],
            "file": ["file"],
            "general": ["search", "retrieval", "calculation"],
        }

        # Core tools that should be prioritized for each task type
        # 每种 task 类型应优先选择的核心 tool
        TASK_CORE_TOOLS = {
            "research": ["web_search", "web_fetch", "web_subpage_fetch", "x_search"],
            "coding": ["python_executor", "file_read", "file_write", "file_edit"],
            "analysis": ["calculator", "advanced_calculator", "python_executor"],
            "browser": ["browser"],
            "file": ["file_read", "file_write", "file_edit", "file_list", "file_search"],
            "general": ["web_search", "calculator", "file_read", "file_write"],
        }

        # Get base tools to filter from
        # 获取用于过滤的基础 tool 集合
        base_tools = set(allowed_tools) if allowed_tools else set(self.list_tools())

        # Get categories and core tools for this task type
        # 获取该 task 类型的分类与核心 tool
        categories = TASK_CATEGORY_MAP.get(task_type, TASK_CATEGORY_MAP["general"])
        core_tools = TASK_CORE_TOOLS.get(task_type, TASK_CORE_TOOLS["general"])

        result = []

        # First: Add core tools that are in base_tools
        # 第一步：加入 base_tools 中的核心 tool
        for tool_name in core_tools:
            if tool_name in base_tools and tool_name not in result:
                result.append(tool_name)
                if len(result) >= max_tools:
                    break

        # Second: Add category-matching tools
        # 第二步：加入匹配分类的 tool
        if len(result) < max_tools:
            for category in categories:
                category_tools = self.list_tools_by_category(category)
                for tool_name in category_tools:
                    if tool_name in base_tools and tool_name not in result:
                        result.append(tool_name)
                        if len(result) >= max_tools:
                            break
                if len(result) >= max_tools:
                    break

        # Third: Fill remaining slots with other allowed tools
        # 第三步：用其他允许的 tool 填满剩余名额
        if len(result) < max_tools:
            for tool_name in base_tools:
                if tool_name not in result:
                    # Skip dangerous tools in general filling
                    # 通用填充时跳过危险 tool
                    tool = self.get_tool(tool_name)
                    if tool and not tool.metadata.dangerous:
                        result.append(tool_name)
                        if len(result) >= max_tools:
                            break

        logger.debug(f"Filtered tools for task_type={task_type}: {result} (from {len(base_tools)} available)")
        return result

    def __repr__(self) -> str:
        return f"<ToolRegistry: {len(self._tools)} tools in {len(self._categories)} categories>"


# Global registry singleton
# 全局 registry 单例
_global_registry = None


def get_registry() -> ToolRegistry:
    """Get the global tool registry singleton
    获取全局 tool registry 单例"""
    global _global_registry
    if _global_registry is None:
        _global_registry = ToolRegistry()
    return _global_registry
