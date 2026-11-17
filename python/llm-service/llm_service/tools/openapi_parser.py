"""
Minimal OpenAPI 3.x parser for Shannon tool generation.
Supports OpenAPI 3.0 and 3.1 with MVP feature set.
用于 Shannon tool 生成的最小 OpenAPI 3.x 解析器。
支持 OpenAPI 3.0 与 3.1，具备 MVP 功能集。
"""

from typing import Any, Dict, List, Optional
import re
import copy
import ipaddress
import socket


class OpenAPIParseError(Exception):
    """Raised when OpenAPI spec is invalid or unsupported.
    当 OpenAPI 规范无效或不受支持时抛出。"""

    pass


def _is_private_ip(hostname: str) -> bool:
    """
    Check if hostname resolves to a private/internal IP address.
    检查主机名是否解析到私有/内部 IP 地址。

    Blocks:
    - Private IPs (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
    - Loopback (127.0.0.0/8, ::1)
    - Link-local (169.254.0.0/16, fe80::/10)
    - Cloud metadata (169.254.169.254, metadata.google.internal)
    拦截：
    - 私有 IP（10.0.0.0/8、172.16.0.0/12、192.168.0.0/16）
    - 回环地址（127.0.0.0/8、::1）
    - 链路本地地址（169.254.0.0/16、fe80::/10）
    - 云 metadata 地址（169.254.169.254、metadata.google.internal）

    Uses getaddrinfo to check ALL IP addresses (prevents DNS round-robin bypass).
    使用 getaddrinfo 检查所有 IP 地址（防止 DNS 轮询绕过）。
    """
    # Block known metadata hostnames
    # 拦截已知的 metadata 主机名
    if hostname.lower() in ["metadata.google.internal", "metadata", "169.254.169.254"]:
        return True

    try:
        # Use getaddrinfo to get ALL IP addresses (prevents DNS round-robin bypass)
        # 使用 getaddrinfo 获取所有 IP 地址（防止 DNS 轮询绕过）
        addr_info = socket.getaddrinfo(hostname, None)

        # Check each resolved IP address
        # 检查每个解析出的 IP 地址
        for family, socktype, proto, canonname, sockaddr in addr_info:
            ip_str = sockaddr[0]
            try:
                ip = ipaddress.ip_address(ip_str)

                # If ANY IP is private/reserved, block the hostname
                # 只要任一 IP 为私有/保留地址，就拦截该主机名
                if (
                    ip.is_private
                    or ip.is_loopback
                    or ip.is_link_local
                    or ip.is_reserved
                    or ip.is_multicast
                ):
                    return True
            except ValueError:
                # Invalid IP format - skip this entry
                # 无效 IP 格式——跳过此条目
                continue

        # All IPs checked and none are private
        # 所有 IP 均已检查，且均非私有地址
        return False
    except (socket.gaierror, ValueError):
        # Can't resolve or invalid hostname - allow (will fail later with proper error)
        # 无法解析或主机名无效——放行（后续会以明确错误失败）
        return False


def resolve_ref(spec: Dict[str, Any], ref_path: str) -> Any:
    """
    Resolve a $ref pointer in the OpenAPI spec.
    解析 OpenAPI 规范中的 $ref 指针。

    Args:
        spec: Full OpenAPI specification
            完整的 OpenAPI 规范
        ref_path: Reference path like "#/components/schemas/Pet"
            引用路径，如 "#/components/schemas/Pet"

    Returns:
        The resolved object from the spec
            从规范中解析出的对象

    Raises:
        OpenAPIParseError: If reference cannot be resolved
        OpenAPIParseError：引用无法解析时抛出
    """
    if not ref_path.startswith("#/"):
        raise OpenAPIParseError(
            f"Only local references supported (starting with #/), got: {ref_path}"
        )

    parts = ref_path[2:].split("/")  # Skip "#/" 注：跳过 "#/"
    current = spec

    try:
        for part in parts:
            # Handle escaped characters in JSON Pointer (RFC 6901)
            # 处理 JSON Pointer 中的转义字符（RFC 6901）
            part = part.replace("~1", "/").replace("~0", "~")
            current = current[part]
        return current
    except (KeyError, TypeError) as e:
        raise OpenAPIParseError(f"Failed to resolve $ref '{ref_path}': {e}")


def resolve_refs_in_schema(
    schema: Any, spec: Dict[str, Any], visited: Optional[set] = None
) -> Any:
    """
    Recursively resolve all $ref references in a schema.
    递归解析 schema 中的所有 $ref 引用。

    Args:
        schema: Schema object (may contain $ref)
            schema 对象（可能含 $ref）
        spec: Full OpenAPI specification
            完整的 OpenAPI 规范
        visited: Set of visited $ref paths to detect cycles
            已访问 $ref 路径集合，用于检测循环

    Returns:
        Schema with all $ref references resolved
            所有 $ref 引用均已解析的 schema

    Raises:
        OpenAPIParseError: If circular reference detected
        OpenAPIParseError：检测到循环引用时抛出
    """
    if visited is None:
        visited = set()

    if not isinstance(schema, dict):
        return schema

    # Check for $ref
    # 检查 $ref
    if "$ref" in schema:
        ref_path = schema["$ref"]

        # Detect circular references
        # 检测循环引用
        if ref_path in visited:
            raise OpenAPIParseError(f"Circular reference detected: {ref_path}")

        visited.add(ref_path)
        try:
            # Resolve the reference
            # 解析引用
            resolved = resolve_ref(spec, ref_path)

            # Deep copy to avoid modifying the original spec
            # 深拷贝以避免修改原始规范
            resolved = copy.deepcopy(resolved)

            # Recursively resolve nested refs
            # 递归解析嵌套引用
            resolved = resolve_refs_in_schema(resolved, spec, visited)
        finally:
            # Always cleanup visited set, even on exception
            # 即使发生 exception 也始终清理 visited 集合
            visited.discard(ref_path)

        # Merge properties from the original schema (excluding $ref)
        # OpenAPI 3.1 allows sibling properties alongside $ref
        # 合并原始 schema 中的属性（不含 $ref）
        # OpenAPI 3.1 允许 $ref 旁边存在同级属性
        result = resolved
        for key, value in schema.items():
            if key != "$ref":
                result[key] = value

        return result

    # Recursively resolve refs in nested structures
    # 递归解析嵌套结构中的引用
    result = {}
    for key, value in schema.items():
        if isinstance(value, dict):
            result[key] = resolve_refs_in_schema(value, spec, visited)
        elif isinstance(value, list):
            result[key] = [
                resolve_refs_in_schema(item, spec, visited)
                if isinstance(item, dict)
                else item
                for item in value
            ]
        else:
            result[key] = value

    return result


def validate_spec(spec: Dict[str, Any]) -> None:
    """
    Validate minimal required fields for OpenAPI 3.x spec.
    校验 OpenAPI 3.x 规范的最小必需字段。

    Raises:
        OpenAPIParseError: If spec is invalid or unsupported
        OpenAPIParseError：规范无效或不受支持时抛出
    """
    if not isinstance(spec, dict):
        raise OpenAPIParseError("Spec must be a dictionary")

    # Check OpenAPI version
    # 检查 OpenAPI 版本
    version = spec.get("openapi", "")
    if not version.startswith("3."):
        raise OpenAPIParseError(
            f"Unsupported OpenAPI version: {version}. Only 3.x supported."
        )

    # Check required top-level fields
    # 检查必需的顶层字段
    if "info" not in spec:
        raise OpenAPIParseError("Missing required field: info")
    if "paths" not in spec or not spec["paths"]:
        raise OpenAPIParseError("Missing or empty paths")

    # Validate servers if present
    # 若存在 servers 则校验
    servers = spec.get("servers", [])
    if servers and not isinstance(servers, list):
        raise OpenAPIParseError("servers must be an array")


def extract_base_url(
    spec: Dict[str, Any],
    override_base_url: Optional[str] = None,
    spec_url: Optional[str] = None,
) -> str:
    """
    Extract base URL from OpenAPI spec.
    从 OpenAPI 规范中提取基础 URL。

    Args:
        spec: OpenAPI specification dict
            OpenAPI 规范字典
        override_base_url: Optional override from config
            配置中的可选覆盖值
        spec_url: Optional URL where spec was fetched from (for relative server URLs)
            抓取规范的可选来源 URL（用于解析相对 server URL）

    Returns:
        Base URL for API requests
            用于 API request 的基础 URL
    """
    from urllib.parse import urlparse

    # SSRF protection: Validate override_base_url BEFORE using it
    # SSRF 防护：使用 override_base_url 前先校验
    if override_base_url:
        parsed = urlparse(override_base_url)
        if parsed.hostname and _is_private_ip(parsed.hostname):
            raise OpenAPIParseError(
                f"Override base URL '{override_base_url}' resolves to private/internal IP address. "
                "This is blocked for security (SSRF protection)."
            )
        return override_base_url.rstrip("/")

    # SSRF protection: Validate spec_url BEFORE using it to resolve relative URLs
    # SSRF 防护：使用 spec_url 解析相对 URL 前先校验
    if spec_url:
        parsed = urlparse(spec_url)
        if parsed.hostname and _is_private_ip(parsed.hostname):
            raise OpenAPIParseError(
                f"Spec URL '{spec_url}' resolves to private/internal IP address. "
                "This is blocked for security (SSRF protection)."
            )

    servers = spec.get("servers", [])
    if not servers:
        raise OpenAPIParseError(
            "No servers defined in spec and no base_url override provided"
        )

    # Use first server
    # 使用第一个 server
    server = servers[0]
    url = server.get("url", "")
    if not url:
        raise OpenAPIParseError("Server URL is empty")

    # Handle server variables (use defaults, validate BEFORE substitution)
    # 处理 server 变量（使用默认值，替换前先校验）
    variables = server.get("variables", {})
    for var_name, var_spec in variables.items():
        default = var_spec.get("default", "")
        # SSRF protection: Validate variable defaults if they look like URLs
        # SSRF 防护：若变量默认值形似 URL 则校验
        if default and ("://" in default or default.startswith("/")):
            parsed = urlparse(default)
            if parsed.hostname and _is_private_ip(parsed.hostname):
                raise OpenAPIParseError(
                    f"Server variable '{var_name}' default value '{default}' resolves to private/internal IP address. "
                    "This is blocked for security (SSRF protection)."
                )
        placeholder = "{" + var_name + "}"
        url = url.replace(placeholder, default)

    # Handle relative URLs (common in PetStore and other examples)
    # 处理相对 URL（常见于 PetStore 等示例）
    if url.startswith("/"):
        if spec_url:
            # Extract scheme://host from spec_url (already validated above)
            # 从 spec_url 提取 scheme://host（上方已校验）
            parsed = urlparse(spec_url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            url = base + url
        else:
            raise OpenAPIParseError(
                f"Server URL '{url}' is relative but no spec_url provided to resolve it"
            )

    # Final SSRF protection: Block private/internal IPs after all resolution
    # This is a defense-in-depth check in case any edge case slipped through
    # 最终 SSRF 防护：所有解析完成后拦截私有/内部 IP
    # 这是纵深防御检查，防止任何边界情况漏过
    parsed_url = urlparse(url)
    hostname = parsed_url.hostname
    if hostname and _is_private_ip(hostname):
        raise OpenAPIParseError(
            f"Server URL '{url}' resolves to private/internal IP address. "
            "This is blocked for security (SSRF protection). "
            "Public APIs only."
        )

    return url.rstrip("/")


def extract_operations(
    spec: Dict[str, Any],
    operations_filter: Optional[List[str]] = None,
    tags_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Extract operations from OpenAPI spec.
    从 OpenAPI 规范中提取操作。

    Args:
        spec: OpenAPI specification dict
            OpenAPI 规范字典
        operations_filter: Optional list of operationIds to include
            可选的要包含的 operationId 列表
        tags_filter: Optional list of tags to filter by
            可选的用于过滤的标签列表

    Returns:
        List of operation dicts with keys: method, path, operation, operation_id
        操作字典列表，键为：method、path、operation、operation_id
    """
    operations = []
    paths = spec.get("paths", {})

    http_methods = ["get", "post", "put", "patch", "delete", "head", "options"]

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue

        for method in http_methods:
            if method not in path_item:
                continue

            operation = path_item[method]
            if not isinstance(operation, dict):
                continue

            # Get or generate operationId
            # 获取或生成 operationId
            operation_id = operation.get("operationId")
            if not operation_id:
                # Generate stable name: method_path (sanitized)
                # 生成稳定名称：method_path（已净化）
                sanitized_path = re.sub(r"[^a-zA-Z0-9_]", "_", path.strip("/"))
                operation_id = f"{method}_{sanitized_path}"

            # Filter by operationId if specified
            # 若指定了 operationId 则过滤
            if operations_filter and operation_id not in operations_filter:
                continue

            # Filter by tags if specified
            # 若指定了标签则过滤
            if tags_filter:
                op_tags = operation.get("tags", [])
                if not any(tag in tags_filter for tag in op_tags):
                    continue

            operations.append(
                {
                    "method": method.upper(),
                    "path": path,
                    "operation": operation,
                    "operation_id": operation_id,
                }
            )

    # Check for explosion
    # 检查数量是否爆炸
    if len(operations) > 200:
        raise OpenAPIParseError(
            f"Spec contains {len(operations)} operations. "
            "Use operations or tags filter to limit scope (max 200)."
        )

    return operations


def map_openapi_type_to_tool_type(
    openapi_type: str, openapi_format: Optional[str] = None
) -> str:
    """
    Map OpenAPI primitive types to Shannon ToolParameterType.
    将 OpenAPI 原始类型映射为 Shannon ToolParameterType。

    Args:
        openapi_type: OpenAPI type (string, integer, number, boolean, array, object)
            OpenAPI 类型（string、integer、number、boolean、array、object）
        openapi_format: Optional format (int32, int64, float, double, etc.)
            可选格式（int32、int64、float、double 等）

    Returns:
        Shannon ToolParameterType name
        Shannon ToolParameterType 名称
    """
    return {
        "string": "string",
        "integer": "integer",
        "number": "float",
        "boolean": "boolean",
        "array": "array",
        "object": "object",
    }.get(openapi_type, "string")


def extract_parameters(
    operation: Dict[str, Any], spec: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Extract path and query parameters from operation.
    MVP: Only support primitive types (string, integer, number, boolean).
    从操作中提取路径与查询参数。
    MVP：仅支持原始类型（string、integer、number、boolean）。

    Args:
        operation: OpenAPI operation object
            OpenAPI 操作对象
        spec: Full OpenAPI spec (for resolving $ref)
            完整 OpenAPI 规范（用于解析 $ref）

    Returns:
        List of parameter dicts with keys: name, type, required, description, location
        参数字典列表，键为：name、type、required、description、location
    """
    params = []

    # Extract path and query parameters
    # 提取路径与查询参数
    parameters = operation.get("parameters", [])
    for param in parameters:
        if not isinstance(param, dict):
            continue

        # Resolve $ref if present
        # 若存在 $ref 则解析
        if "$ref" in param:
            try:
                param = resolve_refs_in_schema(param, spec)
            except OpenAPIParseError:
                # Skip parameters we can't resolve
                # 跳过无法解析的参数
                continue

        location = param.get("in", "")
        if location not in ["path", "query", "header"]:
            # Skip cookie params (header params are now extracted)
            # 跳过 cookie 参数（header 参数现已提取）
            continue

        name = param.get("name")
        if not name:
            continue

        required = param.get("required", False)
        if location == "path":
            required = True  # Path params are always required 注：路径参数始终必填

        description = param.get("description", "")

        # Get schema (resolve $ref if present)
        # 获取 schema（若存在 $ref 则解析）
        schema = param.get("schema", {})
        if "$ref" in schema:
            try:
                schema = resolve_refs_in_schema(schema, spec)
            except OpenAPIParseError:
                # Fallback to string type if ref can't be resolved
                # 无法解析 ref 时回退为 string 类型
                schema = {"type": "string"}

        param_type = schema.get("type", "string")
        param_format = schema.get("format")

        # Map to tool type
        # 映射为 tool 类型
        tool_type = map_openapi_type_to_tool_type(param_type, param_format)

        # Extract enum if present
        # 若存在枚举则提取
        enum_values = schema.get("enum")

        params.append(
            {
                "name": name,
                "type": tool_type,
                "required": required,
                "description": description,
                "location": location,
                "enum": enum_values,
            }
        )

    return params


def extract_request_body(
    operation: Dict[str, Any], spec: Dict[str, Any]
) -> Optional[List[Dict[str, Any]]]:
    """
    Extract request body parameter from operation.
    MVP: Only support application/json content type.
    从操作中提取 request body 参数。
    MVP：仅支持 application/json 内容类型。

    Args:
        operation: OpenAPI operation object
            OpenAPI 操作对象
        spec: Full OpenAPI specification (for resolving $ref)
            完整的 OpenAPI 规范（用于解析 $ref）

    Returns:
        List with single body parameter, or None if no body
        仅含单个 body 参数的列表；无 request body 时返回 None
    """
    request_body = operation.get("requestBody")
    if not request_body:
        return None

    content = request_body.get("content", {})

    # Check for application/json
    # 检查 application/json
    json_content = content.get("application/json")
    if not json_content:
        # No JSON body
        # 无 JSON request body
        return None

    required = request_body.get("required", False)
    description = request_body.get("description", "Request body (send as-is, no wrapping)")

    # Return single body parameter - LLM should provide complete object structure
    # The tool execution will send this directly as json= without wrapping
    # 返回单个 body 参数——LLM 应提供完整的对象结构
    # tool 执行时会直接以 json= 发送，不做包装
    return [{
        "name": "body",
        "type": "object",
        "required": required,
        "description": description,
        "location": "body",
    }]


def deduplicate_operation_ids(operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate operation IDs by appending numeric suffixes.
    通过追加数字后缀去重 operation ID。

    Args:
        operations: List of operation dicts
        操作字典列表

    Returns:
        Operations with unique operation_ids
        operation_id 唯一的操作列表
    """
    seen = {}
    result = []

    for op in operations:
        op_id = op["operation_id"]
        original_id = op_id

        # Check for collision
        # 检查冲突
        if op_id in seen:
            counter = 2
            while f"{original_id}_{counter}" in seen:
                counter += 1
            op_id = f"{original_id}_{counter}"
            op["operation_id"] = op_id

        seen[op_id] = True
        result.append(op)

    return result
