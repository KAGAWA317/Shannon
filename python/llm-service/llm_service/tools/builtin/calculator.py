"""
Calculator Tool - Safe mathematical expression evaluation
Calculator tool——安全的数学表达式求值
"""

import ast
import operator
import math
from typing import List, Any

from typing import Dict, Optional

from ..base import Tool, ToolMetadata, ToolParameter, ToolParameterType, ToolResult
from ...config import Settings


# Safe operators for expression evaluation
# 用于表达式求值的安全运算符
SAFE_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
    ast.Mod: operator.mod,
    ast.FloorDiv: operator.floordiv,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

# Safe math functions
# 安全的数学函数
SAFE_FUNCTIONS = {
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "sum": sum,
    "len": len,
    "sqrt": math.sqrt,
    "sin": math.sin,
    "cos": math.cos,
    "tan": math.tan,
    "log": math.log,
    "log10": math.log10,
    "exp": math.exp,
    "pow": pow,
    "pi": math.pi,
    "e": math.e,
    "floor": math.floor,
    "ceil": math.ceil,
}


class CalculatorTool(Tool):
    """
    Safe calculator that can evaluate mathematical expressions.
    可对数学表达式求值的安全计算器。
    Uses AST parsing to ensure safety - no arbitrary code execution.
    使用 AST 解析保证安全——不会执行任意代码。
    """

    def __init__(self):
        self.settings = Settings()
        super().__init__()

    def _get_metadata(self) -> ToolMetadata:
        return ToolMetadata(
            name="calculator",
            version="1.0.0",
            description="Evaluate mathematical expressions safely",
            category="calculation",
            author="Shannon",
            requires_auth=False,
            rate_limit=self.settings.calculator_rate_limit,  # Configurable via CALCULATOR_RATE_LIMIT env var 注：可通过 CALCULATOR_RATE_LIMIT 环境变量配置
            timeout_seconds=5,
            memory_limit_mb=64,
            sandboxed=True,
            dangerous=False,
            cost_per_use=0.0,
        )

    def _get_parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="expression",
                type=ToolParameterType.STRING,
                description="Mathematical expression to evaluate (e.g., '2 + 2', 'sqrt(16)', 'sin(pi/2)')",
                required=True,
            ),
            ToolParameter(
                name="precision",
                type=ToolParameterType.INTEGER,
                description="Number of decimal places for the result",
                required=False,
                default=6,
                min_value=0,
                max_value=15,
            ),
        ]

    async def _execute_impl(
        self, session_context: Optional[Dict] = None, **kwargs
    ) -> ToolResult:
        """
        Safely evaluate mathematical expression
        安全地对数学表达式求值
        """
        expression = kwargs["expression"]
        precision = kwargs.get("precision", 6)

        try:
            # Parse and evaluate the expression safely
            # 安全地解析并求值表达式
            result = self._safe_eval(expression)

            # Format the result based on precision
            # 按 precision 格式化结果
            if isinstance(result, float):
                if precision == 0:
                    result = int(round(result))
                else:
                    result = round(result, precision)

            return ToolResult(
                success=True,
                output=result,
                metadata={
                    "expression": expression,
                    "result_type": type(result).__name__,
                    "precision": precision,
                },
            )

        except ZeroDivisionError:
            return ToolResult(success=False, output=None, error="Division by zero")
        except ValueError as e:
            return ToolResult(
                success=False, output=None, error=f"Math domain error: {str(e)}"
            )
        except Exception as e:
            return ToolResult(
                success=False, output=None, error=f"Invalid expression: {str(e)}"
            )

    def _safe_eval(self, expression: str) -> Any:
        """
        Safely evaluate a mathematical expression using AST parsing.
        使用 AST 解析安全地对数学表达式求值。
        Only allows mathematical operations, no arbitrary code execution.
        仅允许数学运算，不执行任意代码。
        """
        # Parse the expression into an AST
        # 将表达式解析为 AST
        try:
            node = ast.parse(expression, mode="eval")
        except SyntaxError as e:
            raise ValueError(f"Invalid expression syntax: {e}")

        # Evaluate the AST safely
        # 安全地求值 AST
        return self._eval_node(node.body)

    def _eval_node(self, node: ast.AST) -> Any:
        """
        Recursively evaluate an AST node.
        递归求值 AST 节点。
        Only allows safe operations.
        仅允许安全操作。
        """
        if isinstance(node, ast.Constant):  # Python 3.8+ 注：Python 3.8+
            return node.value
        elif isinstance(node, ast.Num):  # For older Python versions 注：兼容更早的 Python 版本
            return node.n
        elif isinstance(node, ast.Name):
            # Allow only safe constants and functions
            # 仅允许安全的常量和函数
            if node.id not in SAFE_FUNCTIONS:
                raise ValueError(f"Unknown variable or function: {node.id}")
            # 常量（如 pi、e）直接返回值，函数返回可调用对象供 ast.Call 使用
            return SAFE_FUNCTIONS[node.id]
        elif isinstance(node, ast.BinOp):
            # Binary operations like +, -, *, /
            # 二元运算，如 +、-、*、/
            if type(node.op) not in SAFE_OPERATORS:
                raise ValueError(f"Unsafe operator: {type(node.op).__name__}")
            left = self._eval_node(node.left)
            right = self._eval_node(node.right)
            return SAFE_OPERATORS[type(node.op)](left, right)
        elif isinstance(node, ast.UnaryOp):
            # Unary operations like -, +
            # 一元运算，如 -、+
            if type(node.op) not in SAFE_OPERATORS:
                raise ValueError(f"Unsafe unary operator: {type(node.op).__name__}")
            operand = self._eval_node(node.operand)
            return SAFE_OPERATORS[type(node.op)](operand)
        elif isinstance(node, ast.Call):
            # Function calls
            # 函数调用
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
                if func_name not in SAFE_FUNCTIONS:
                    raise ValueError(f"Unsafe function: {func_name}")
                func = SAFE_FUNCTIONS[func_name]
                args = [self._eval_node(arg) for arg in node.args]
                return func(*args)
            else:
                raise ValueError("Complex function calls not allowed")
        elif isinstance(node, ast.List):
            # Lists for functions like sum(), min(), max()
            # 供 sum()、min()、max() 等函数使用的列表
            return [self._eval_node(elem) for elem in node.elts]
        elif isinstance(node, ast.Tuple):
            # Tuples
            # 元组
            return tuple(self._eval_node(elem) for elem in node.elts)
        else:
            raise ValueError(f"Unsafe node type: {type(node).__name__}")


class StatisticalCalculatorTool(Tool):
    """
    Advanced statistical calculations
    高级统计计算
    """

    def _get_metadata(self) -> ToolMetadata:
        return ToolMetadata(
            name="statistics",
            version="1.0.0",
            description="Perform statistical calculations on datasets",
            category="calculation",
            author="Shannon",
            requires_auth=False,
            rate_limit=100,
            timeout_seconds=10,
            memory_limit_mb=256,
            sandboxed=True,
            dangerous=False,
            cost_per_use=0.0,
        )

    def _get_parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="data",
                type=ToolParameterType.ARRAY,
                description="Array of numerical values",
                required=True,
            ),
            ToolParameter(
                name="operation",
                type=ToolParameterType.STRING,
                description="Statistical operation to perform",
                required=True,
                enum=[
                    "mean",
                    "median",
                    "mode",
                    "std",
                    "variance",
                    "min",
                    "max",
                    "sum",
                    "count",
                ],
            ),
        ]

    async def _execute_impl(
        self, session_context: Optional[Dict] = None, **kwargs
    ) -> ToolResult:
        """
        Perform statistical calculation
        执行统计计算
        """
        import statistics

        data = kwargs["data"]
        operation = kwargs["operation"]

        if not data:
            return ToolResult(success=False, output=None, error="Empty dataset")

        try:
            # Ensure all values are numeric
            # 确保所有值均为数值
            numeric_data = [float(x) for x in data]

            result = None
            if operation == "mean":
                result = statistics.mean(numeric_data)
            elif operation == "median":
                result = statistics.median(numeric_data)
            elif operation == "mode":
                try:
                    result = statistics.mode(numeric_data)
                except statistics.StatisticsError:
                    result = "No unique mode"
            elif operation == "std":
                if len(numeric_data) > 1:
                    result = statistics.stdev(numeric_data)
                else:
                    result = 0.0
            elif operation == "variance":
                if len(numeric_data) > 1:
                    result = statistics.variance(numeric_data)
                else:
                    result = 0.0
            elif operation == "min":
                result = min(numeric_data)
            elif operation == "max":
                result = max(numeric_data)
            elif operation == "sum":
                result = sum(numeric_data)
            elif operation == "count":
                result = len(numeric_data)

            return ToolResult(
                success=True,
                output=result,
                metadata={
                    "operation": operation,
                    "data_points": len(numeric_data),
                    "data_range": [min(numeric_data), max(numeric_data)]
                    if numeric_data
                    else [None, None],
                },
            )

        except ValueError as e:
            return ToolResult(
                success=False, output=None, error=f"Invalid data: {str(e)}"
            )
        except Exception as e:
            return ToolResult(
                success=False, output=None, error=f"Calculation error: {str(e)}"
            )
