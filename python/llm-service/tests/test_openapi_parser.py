"""
Unit tests for OpenAPI parser with $ref resolution.
带 $ref 解析的 OpenAPI parser 单元测试。
"""

import pytest
from llm_service.tools.openapi_parser import (
    OpenAPIParseError,
    extract_base_url,
    extract_parameters,
    resolve_ref,
    resolve_refs_in_schema,
    validate_spec,
)


class TestRefResolution:
    """Test $ref resolution functionality.

    测试 $ref 解析功能。
    """

    def test_simple_ref_resolution(self):
        """Test basic $ref pointer resolution.

        测试基础的 $ref 指针解析。
        """
        spec = {
            "components": {
                "schemas": {
                    "Pet": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    }
                }
            }
        }

        result = resolve_ref(spec, "#/components/schemas/Pet")
        assert result["type"] == "object"
        assert "name" in result["properties"]

    def test_nested_ref_resolution(self):
        """Test resolving nested $ref in schema.

        测试解析 schema 中嵌套的 $ref。
        """
        spec = {
            "components": {
                "schemas": {
                    "Address": {
                        "type": "object",
                        "properties": {"street": {"type": "string"}},
                    },
                    "Person": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "address": {"$ref": "#/components/schemas/Address"},
                        },
                    },
                }
            }
        }

        schema = {"$ref": "#/components/schemas/Person"}
        result = resolve_refs_in_schema(schema, spec)

        assert result["type"] == "object"
        assert "name" in result["properties"]
        assert "address" in result["properties"]
        assert result["properties"]["address"]["type"] == "object"
        assert "street" in result["properties"]["address"]["properties"]

    def test_circular_ref_detection(self):
        """Test circular reference detection.

        测试循环引用检测。
        """
        spec = {
            "components": {
                "schemas": {
                    "Node": {
                        "type": "object",
                        "properties": {
                            "value": {"type": "string"},
                            "next": {"$ref": "#/components/schemas/Node"},
                        },
                    }
                }
            }
        }

        schema = {"$ref": "#/components/schemas/Node"}

        # Should raise error due to circular reference
        # 循环引用应抛出错误
        with pytest.raises(OpenAPIParseError, match="Circular reference"):
            resolve_refs_in_schema(schema, spec)

    def test_invalid_ref_path(self):
        """Test handling of invalid $ref path.

        测试无效 $ref 路径的处理。
        """
        spec = {"components": {"schemas": {}}}

        with pytest.raises(OpenAPIParseError, match="Failed to resolve"):
            resolve_ref(spec, "#/components/schemas/NonExistent")

    def test_external_ref_rejection(self):
        """Test rejection of external $ref (not supported in MVP).

        测试拒绝外部 $ref（MVP 不支持）。
        """
        spec = {}

        with pytest.raises(OpenAPIParseError, match="Only local references supported"):
            resolve_ref(spec, "https://example.com/schema.json#/Pet")

    def test_rfc6901_escaping(self):
        """Test RFC 6901 JSON Pointer escaping (~0 for ~, ~1 for /).

        测试 RFC 6901 JSON Pointer 转义（~0 表示 ~，~1 表示 /）。
        """
        spec = {
            "components": {
                "schemas": {
                    "Foo~Bar": {"type": "string"},
                    "Foo/Bar": {"type": "number"},
                }
            }
        }

        # ~0 should decode to ~
        # ~0 应解码为 ~
        result1 = resolve_ref(spec, "#/components/schemas/Foo~0Bar")
        assert result1["type"] == "string"

        # ~1 should decode to /
        # ~1 应解码为 /
        result2 = resolve_ref(spec, "#/components/schemas/Foo~1Bar")
        assert result2["type"] == "number"

    def test_sibling_properties_with_ref(self):
        """Test that sibling properties alongside $ref are merged (OpenAPI 3.1).

        测试 $ref 旁的兄弟属性会被合并（OpenAPI 3.1）。
        """
        spec = {
            "components": {
                "schemas": {
                    "Pet": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    }
                }
            }
        }

        schema = {
            "$ref": "#/components/schemas/Pet",
            "description": "A pet with custom description",
        }

        result = resolve_refs_in_schema(schema, spec)

        assert result["type"] == "object"
        assert "name" in result["properties"]
        assert result["description"] == "A pet with custom description"

    def test_array_with_ref_items(self):
        """Test resolving $ref in array items.

        测试解析 array items 中的 $ref。
        """
        spec = {
            "components": {
                "schemas": {
                    "Tag": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    }
                }
            }
        }

        schema = {"type": "array", "items": {"$ref": "#/components/schemas/Tag"}}

        result = resolve_refs_in_schema(schema, spec)

        assert result["type"] == "array"
        assert result["items"]["type"] == "object"
        assert "name" in result["items"]["properties"]


class TestParameterExtraction:
    """Test parameter extraction with $ref resolution.

    测试带 $ref 解析的 parameter 提取。
    """

    def test_parameter_ref_resolution(self):
        """Test extracting parameters that use $ref.

        测试提取使用 $ref 的 parameters。
        """
        spec = {
            "components": {
                "parameters": {
                    "PageParam": {
                        "name": "page",
                        "in": "query",
                        "schema": {"type": "integer", "minimum": 1},
                    }
                }
            }
        }

        operation = {"parameters": [{"$ref": "#/components/parameters/PageParam"}]}

        params = extract_parameters(operation, spec)

        assert len(params) == 1
        assert params[0]["name"] == "page"
        assert params[0]["location"] == "query"
        assert params[0]["type"] == "integer"

    def test_schema_ref_in_parameter(self):
        """Test parameter with schema containing $ref.

        测试 parameter 中 schema 含 $ref 的情况。
        """
        spec = {
            "components": {
                "schemas": {
                    "Status": {"type": "string", "enum": ["active", "inactive"]}
                }
            }
        }

        operation = {
            "parameters": [
                {
                    "name": "status",
                    "in": "query",
                    "schema": {"$ref": "#/components/schemas/Status"},
                }
            ]
        }

        params = extract_parameters(operation, spec)

        assert len(params) == 1
        assert params[0]["name"] == "status"
        assert params[0]["enum"] == ["active", "inactive"]

    def test_invalid_parameter_ref_skipped(self):
        """Test that invalid parameter $ref is gracefully skipped.

        测试无效的 parameter $ref 会被优雅跳过。
        """
        spec = {"components": {"parameters": {}}}

        operation = {
            "parameters": [
                {"$ref": "#/components/parameters/NonExistent"},
                {"name": "valid", "in": "query", "schema": {"type": "string"}},
            ]
        }

        params = extract_parameters(operation, spec)

        # Should skip invalid ref but include valid parameter
        # 应跳过无效 ref，但保留有效 parameter
        assert len(params) == 1
        assert params[0]["name"] == "valid"


class TestSpecValidation:
    """Test OpenAPI spec validation.

    测试 OpenAPI spec 校验。
    """

    def test_valid_spec_30(self):
        """Test validation of valid OpenAPI 3.0 spec.

        测试合法 OpenAPI 3.0 spec 的校验。
        """
        spec = {
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "paths": {"/test": {"get": {"summary": "Test endpoint"}}},
        }

        # Should not raise
        # 不应抛 exception
        validate_spec(spec)

    def test_valid_spec_31(self):
        """Test validation of valid OpenAPI 3.1 spec.

        测试合法 OpenAPI 3.1 spec 的校验。
        """
        spec = {
            "openapi": "3.1.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "paths": {"/test": {"get": {"summary": "Test endpoint"}}},
        }

        # Should not raise
        # 不应抛 exception
        validate_spec(spec)

    def test_invalid_version(self):
        """Test rejection of unsupported OpenAPI version.

        测试拒绝不支持的 OpenAPI 版本。
        """
        spec = {"openapi": "2.0", "info": {"title": "Test API", "version": "1.0.0"}}

        with pytest.raises(
            OpenAPIParseError, match="Unsupported OpenAPI version.*Only 3"
        ):
            validate_spec(spec)

    def test_missing_required_fields(self):
        """Test rejection of spec missing required fields.

        测试拒绝缺少必填字段的 spec。
        """
        spec = {"openapi": "3.0.0"}

        with pytest.raises(OpenAPIParseError):
            validate_spec(spec)


class TestSSRFProtection:
    """Test SSRF (Server-Side Request Forgery) protection.

    测试 SSRF（Server-Side Request Forgery）防护。
    """

    def test_blocks_aws_metadata_ip(self):
        """Test blocking AWS EC2 metadata service IP.

        测试拦截 AWS EC2 metadata service IP。
        """
        spec = {
            "openapi": "3.0.0",
            "info": {"title": "Malicious API", "version": "1.0"},
            "paths": {},
            "servers": [{"url": "http://169.254.169.254/latest/meta-data/"}],
        }

        with pytest.raises(OpenAPIParseError, match="private/internal IP.*SSRF"):
            extract_base_url(spec)

    def test_blocks_localhost(self):
        """Test blocking localhost.

        测试拦截 localhost。
        """
        spec = {
            "openapi": "3.0.0",
            "info": {"title": "Malicious API", "version": "1.0"},
            "paths": {},
            "servers": [{"url": "http://localhost:8080"}],
        }

        with pytest.raises(OpenAPIParseError, match="private/internal IP.*SSRF"):
            extract_base_url(spec)

    def test_blocks_private_network(self):
        """Test blocking private network IPs.

        测试拦截私有网络 IP。
        """
        spec = {
            "openapi": "3.0.0",
            "info": {"title": "Malicious API", "version": "1.0"},
            "paths": {},
            "servers": [{"url": "http://192.168.1.1"}],
        }

        with pytest.raises(OpenAPIParseError, match="private/internal IP.*SSRF"):
            extract_base_url(spec)

    def test_allows_public_ip(self):
        """Test allowing legitimate public APIs.

        测试放行合法的公网 API。
        """
        spec = {
            "openapi": "3.0.0",
            "info": {"title": "Public API", "version": "1.0"},
            "paths": {},
            "servers": [{"url": "https://petstore.swagger.io/v2"}],
        }

        # Should not raise
        # 不应抛 exception
        url = extract_base_url(spec)
        assert url == "https://petstore.swagger.io/v2"

    def test_blocks_metadata_hostname(self):
        """Test blocking GCP metadata hostname.

        测试拦截 GCP metadata 主机名。
        """
        spec = {
            "openapi": "3.0.0",
            "info": {"title": "Malicious API", "version": "1.0"},
            "paths": {},
            "servers": [{"url": "http://metadata.google.internal"}],
        }

        with pytest.raises(OpenAPIParseError, match="private/internal IP.*SSRF"):
            extract_base_url(spec)
