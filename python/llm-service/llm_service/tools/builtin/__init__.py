"""
Built-in tools for Shannon platform
Shannon 平台的内置 tool
"""

from .bash_executor import BashExecutorTool
from .calculator import CalculatorTool
from .data_tools import DiffFilesTool, JsonQueryTool
from .file_ops import (
    FileDeleteTool,
    FileEditTool,
    FileListTool,
    FileReadTool,
    FileSearchTool,
    FileWriteTool,
)
from .python_wasi_executor import PythonWasiExecutorTool
from .web_crawl import WebCrawlTool
from .web_fetch import WebFetchTool
from .web_search import WebSearchTool
from .web_subpage_fetch import WebSubpageFetchTool
from .x_search import XSearchTool

# Browser automation tool
# 浏览器自动化 tool
try:
    from .browser_use import BrowserTool  # noqa: F401  # availability probe 注：仅用于探测可用性
    _HAS_BROWSER_TOOLS = True
except ImportError:
    _HAS_BROWSER_TOOLS = False

__all__ = [
    "WebSearchTool",
    "WebFetchTool",
    "WebSubpageFetchTool",
    "WebCrawlTool",
    "CalculatorTool",
    "FileReadTool",
    "FileWriteTool",
    "FileListTool",
    "FileSearchTool",
    "FileEditTool",
    "FileDeleteTool",
    "DiffFilesTool",
    "JsonQueryTool",
    "BashExecutorTool",
    "PythonWasiExecutorTool",
    "XSearchTool",
]

# Add browser tool to exports if available
# 若可用则把 browser 工具加入 exports
if _HAS_BROWSER_TOOLS:
    __all__.append("BrowserTool")
