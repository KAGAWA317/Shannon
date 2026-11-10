from llm_service.roles.presets import render_system_prompt


def test_pass_through_when_no_placeholders():
    prompt = "You are a helpful AI assistant."
    out = render_system_prompt(prompt, {"prompt_params": {"target": "./src"}})
    assert out == prompt


def test_missing_variables_render_empty_strings():
    prompt = "Hello ${name}, analyze ${target}"
    out = render_system_prompt(prompt, {"prompt_params": {"target": "./src"}})
    assert out == "Hello , analyze ./src"


def test_tool_parameters_not_used_for_rendering():
    """Verify that tool_parameters is NOT used for variable substitution (only prompt_params).
    验证 tool_parameters 不参与变量替换（只有 prompt_params 参与）。
    """
    prompt = "Analyze ${target} at depth ${depth}"
    ctx = {
        "tool_parameters": {"target": "A", "depth": "shallow"},
        "prompt_params": {"target": "B"},
    }
    out = render_system_prompt(prompt, ctx)
    # tool_parameters["depth"] should NOT be used; missing vars become empty
    # 不应使用 tool_parameters["depth"]；缺失的变量渲染为空字符串
    assert out == "Analyze B at depth "


def test_non_whitelisted_context_not_used():
    prompt = "Role=${role} Target=${target}"
    ctx = {
        "role": "analyst",
        "prompt_params": {"target": "./service"},
    }
    out = render_system_prompt(prompt, ctx)
    # role is not whitelisted; should collapse to empty
    # role 不在白名单内；应折叠为空字符串
    assert out == "Role= Target=./service"

