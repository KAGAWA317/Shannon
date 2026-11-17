"""Tests for swarm prompt organization in roles/swarm/.

TDD: These tests define the expected structure and content of swarm prompts.
Write tests first, then implement the prompts to make them pass.

按 TDD 写的 prompt 契约测试：先定下 swarm prompt 该有的结构与内容，
再改 prompt 让它们通过。
"""



class TestAgentProtocol:
    """Tests for AGENT_LOOP_SYSTEM_PROMPT in roles/swarm/agent_protocol.py."""

    def test_import(self):
        """AGENT_LOOP_SYSTEM_PROMPT should be importable from roles.swarm."""
        from llm_service.roles.swarm import AGENT_LOOP_SYSTEM_PROMPT

        assert isinstance(AGENT_LOOP_SYSTEM_PROMPT, str)
        assert len(AGENT_LOOP_SYSTEM_PROMPT) > 500

    def test_has_core_phases(self):
        """Prompt should have ORIENT, EXECUTE, SHARE phases."""
        from llm_service.roles.swarm import AGENT_LOOP_SYSTEM_PROMPT

        assert "PHASE 1" in AGENT_LOOP_SYSTEM_PROMPT
        assert "PHASE 2" in AGENT_LOOP_SYSTEM_PROMPT
        assert "PHASE 3" in AGENT_LOOP_SYSTEM_PROMPT

    def test_has_only_4_actions(self):
        """Agent should have exactly 4 actions: tool_call, send_message, publish_data, idle."""
        from llm_service.roles.swarm import AGENT_LOOP_SYSTEM_PROMPT

        # These 4 actions MUST be present (done was removed — always converted to idle)
        # 只保留这 4 个 action；done 已移除，运行时一律转成 idle
        for action in ["tool_call", "send_message", "publish_data", "idle"]:
            assert action in AGENT_LOOP_SYSTEM_PROMPT, f"Missing action: {action}"

    def test_removed_actions_not_present(self):
        """Removed actions should NOT be in the prompt as available actions."""
        from llm_service.roles.swarm import AGENT_LOOP_SYSTEM_PROMPT

        # These were removed per plan PRM-3
        # PRM-3 方案里删掉的 action
        # Check they're not listed as numbered actions (they may appear in general text)
        # 只检查编号动作列表：这些词在正文里出现不算
        lines = AGENT_LOOP_SYSTEM_PROMPT.split("\n")
        action_lines = [ln for ln in lines if ln.strip().startswith(("4.", "5.", "6.", "7.", "8.", "9."))]
        for line in action_lines:
            assert "claim_task" not in line, "claim_task should be removed from actions"
            assert "create_task" not in line, "create_task should be removed from actions"
            assert "complete_task" not in line, "complete_task should be removed from actions"
            assert "request_help" not in line, "request_help should be removed from actions"

    def test_quality_check_in_phase3(self):
        """QUALITY SELF-CHECK should be in PHASE 3 section."""
        from llm_service.roles.swarm import AGENT_LOOP_SYSTEM_PROMPT

        assert "QUALITY SELF-CHECK" in AGENT_LOOP_SYSTEM_PROMPT

    def test_json_constraint_only_once(self):
        """JSON format instruction should appear at most once in system prompt."""
        from llm_service.roles.swarm import AGENT_LOOP_SYSTEM_PROMPT

        count = AGENT_LOOP_SYSTEM_PROMPT.count("Return ONLY valid JSON")
        assert count <= 1, f"JSON constraint appears {count} times, should be at most 1"

    def test_decision_summary_required(self):
        """Prompt should require decision_summary in every response."""
        from llm_service.roles.swarm import AGENT_LOOP_SYSTEM_PROMPT

        assert "decision_summary" in AGENT_LOOP_SYSTEM_PROMPT


class TestLeadProtocol:
    """Tests for Lead prompts in roles/swarm/lead_protocol.py."""

    def test_import_lead_system_prompt(self):
        """LEAD_SYSTEM_PROMPT should be importable from roles.swarm."""
        from llm_service.roles.swarm import LEAD_SYSTEM_PROMPT

        assert isinstance(LEAD_SYSTEM_PROMPT, str)
        assert len(LEAD_SYSTEM_PROMPT) > 500

    def test_lead_prompt_not_agent_prompt(self):
        """Lead prompt should NOT contain agent-specific instructions."""
        from llm_service.roles.swarm import LEAD_SYSTEM_PROMPT

        assert "PHASE 1 — ORIENT" not in LEAD_SYSTEM_PROMPT
        assert "PHASE 2 — EXECUTE" not in LEAD_SYSTEM_PROMPT
        assert "file_write" not in LEAD_SYSTEM_PROMPT

    def test_lead_has_management_actions(self):
        """Lead prompt should list management actions."""
        from llm_service.roles.swarm import LEAD_SYSTEM_PROMPT

        for action in ["spawn_agent", "assign_task", "send_message", "done"]:
            assert action in LEAD_SYSTEM_PROMPT, f"Missing Lead action: {action}"

    def test_lead_has_quality_gate(self):
        """Lead prompt should have quality gate for evaluating agent output."""
        from llm_service.roles.swarm import LEAD_SYSTEM_PROMPT

        assert "quality" in LEAD_SYSTEM_PROMPT.lower() or "QUALITY" in LEAD_SYSTEM_PROMPT

    def test_lead_has_event_types(self):
        """Lead prompt should document event types."""
        from llm_service.roles.swarm import LEAD_SYSTEM_PROMPT

        for event in ["initial_plan", "agent_completed", "agent_idle"]:
            assert event in LEAD_SYSTEM_PROMPT, f"Missing event type: {event}"

    def test_lead_has_role_matching_guidance(self):
        """Lead prompt must instruct checking agent role before assign_task."""
        from llm_service.roles.swarm import LEAD_SYSTEM_PROMPT
        assert "role field" in LEAD_SYSTEM_PROMPT.lower() or "ROLE FIELD" in LEAD_SYSTEM_PROMPT, \
            "Lead prompt must reference agent role field for assign_task decisions"
        assert "NEVER assign to researcher" in LEAD_SYSTEM_PROMPT, \
            "Lead prompt must prohibit assigning synthesis/code tasks to researchers"

    def test_lead_no_misleading_automatic_synthesis(self):
        """Lead prompt must NOT say synthesis is automatic (Lead reply is primary path)."""
        from llm_service.roles.swarm import LEAD_SYSTEM_PROMPT
        assert "synthesis is AUTOMATIC" not in LEAD_SYSTEM_PROMPT, \
            "Remove misleading 'synthesis is AUTOMATIC' — Lead reply at closing_checkpoint is the primary output path"


class TestRolePrompts:
    """Tests for SWARM_ROLE_PROMPTS in roles/swarm/role_prompts.py."""

    def test_import(self):
        """SWARM_ROLE_PROMPTS should be importable from roles.swarm."""
        from llm_service.roles.swarm import SWARM_ROLE_PROMPTS

        assert isinstance(SWARM_ROLE_PROMPTS, dict)

    def test_core_roles_present(self):
        """Core roles must be present."""
        from llm_service.roles.swarm import SWARM_ROLE_PROMPTS

        for role in ["researcher", "coder", "analyst", "generalist"]:
            assert role in SWARM_ROLE_PROMPTS, f"Missing core role: {role}"

    def test_lead_role_not_in_agent_prompts(self):
        """'lead' role should NOT be in SWARM_ROLE_PROMPTS (it has its own prompt)."""
        from llm_service.roles.swarm import SWARM_ROLE_PROMPTS

        assert "lead" not in SWARM_ROLE_PROMPTS

    def test_role_prompts_are_strings(self):
        """All role prompts should be strings."""
        from llm_service.roles.swarm import SWARM_ROLE_PROMPTS

        for role, prompt in SWARM_ROLE_PROMPTS.items():
            assert isinstance(prompt, str), f"Role {role} prompt is not a string"

    def test_researcher_has_angle_limit(self):
        """A3: Researcher role must have a numeric limit on search dimensions."""
        from llm_service.roles.swarm.role_prompts import SWARM_ROLE_PROMPTS
        researcher_prompt = SWARM_ROLE_PROMPTS.get("researcher", "")
        import re
        assert re.search(r'\d+-\d+ (angles|dimensions)', researcher_prompt), \
            "Researcher role must specify numeric search limit (e.g. '3-5 dimensions')"

    def test_synthesis_writer_has_file_efficiency(self):
        """B2: synthesis_writer must have file efficiency instructions."""
        from llm_service.roles.swarm.role_prompts import SWARM_ROLE_PROMPTS
        prompt = SWARM_ROLE_PROMPTS.get("synthesis_writer", "")
        assert "file_list" in prompt, \
            "synthesis_writer must reference file_list for workspace discovery"
        assert "file_read" in prompt, \
            "synthesis_writer must reference file_read for reading teammate output"


class TestRoleCatalog:
    """Tests for dynamic role catalog generation."""

    def test_get_swarm_role_catalog(self):
        """get_swarm_role_catalog should return roles available for Lead to assign."""
        from llm_service.roles.swarm import get_swarm_role_catalog

        catalog = get_swarm_role_catalog()
        assert isinstance(catalog, dict)
        # Must include at least core roles
        # 至少要有这几个核心 role
        assert "researcher" in catalog
        assert "analyst" in catalog

    def test_catalog_roles_have_presets(self):
        """Every role in the catalog should have a corresponding preset."""
        from llm_service.roles.presets import get_role_preset
        from llm_service.roles.swarm import get_swarm_role_catalog

        catalog = get_swarm_role_catalog()
        for role_key in catalog:
            preset = get_role_preset(role_key)
            # Should either have a real system_prompt or be generalist
            # 除 generalist 外，catalog 里的 role 都必须有真实 system_prompt
            if role_key != "generalist":
                assert preset.get("system_prompt"), (
                    f"Role '{role_key}' is in catalog but has no preset system_prompt"
                )


class TestLeadPromptSource:
    """Verify lead.py uses LEAD_SYSTEM_PROMPT from lead_protocol.py (not inline).

    确认 lead.py 从 lead_protocol.py import prompt：内联一份会导致两处 prompt 各自漂移。
    """

    def test_lead_endpoint_imports_from_lead_protocol(self):
        """lead.py should import LEAD_SYSTEM_PROMPT from roles.swarm.lead_protocol, not define inline."""
        import ast
        from pathlib import Path

        lead_py = Path(__file__).parent.parent / "llm_service" / "api" / "lead.py"
        source = lead_py.read_text()
        tree = ast.parse(source)

        # Check that LEAD_SYSTEM_PROMPT is imported, not assigned inline
        # 必须是 import 进来的，不能在本文件内联赋值，否则又会冒出第二份 prompt
        has_import = False
        has_inline_def = False
        for node in ast.walk(tree):
            if isinstance(node, (ast.ImportFrom,)):
                for alias in node.names:
                    if alias.name == "LEAD_SYSTEM_PROMPT":
                        has_import = True
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "LEAD_SYSTEM_PROMPT":
                        has_inline_def = True

        assert has_import, "lead.py should import LEAD_SYSTEM_PROMPT from lead_protocol"
        assert not has_inline_def, "lead.py should NOT define LEAD_SYSTEM_PROMPT inline"


class TestAgentSendMessageExamples:
    """Tests for send_message few-shot examples in agent protocol."""

    def test_agent_protocol_has_send_message_examples(self):
        """Agent protocol must have >=4 send_message few-shot examples to activate cross-agent comms.

        few-shot 示例是激活 agent 间通信最有效的手段，比抽象规则管用。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        send_msg_count = AGENT_LOOP_SYSTEM_PROMPT.count('"action": "send_message"')
        assert send_msg_count >= 2, (
            f"Expected >=2 send_message examples in AGENT_LOOP_SYSTEM_PROMPT, found {send_msg_count}. "
            "Few-shot examples are the most effective way to activate agent-to-agent communication."
        )

    def test_agent_protocol_has_lead_escalation(self):
        """Agent protocol should show agents can message 'lead' for escalation."""
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert '"to": "lead"' in AGENT_LOOP_SYSTEM_PROMPT, (
            "Agent protocol must have a send_message example with to='lead' for escalation."
        )


class TestLeadAdaptivePlanning:
    """Tests for ADAPTIVE PLANNING section in Lead protocol."""

    def test_lead_protocol_has_adaptive_planning_guidance(self):
        """Lead protocol should encourage dynamic plan revision based on agent findings."""
        from llm_service.roles.swarm.lead_protocol import LEAD_SYSTEM_PROMPT
        assert "ADAPTIVE PLANNING" in LEAD_SYSTEM_PROMPT, (
            "Lead protocol should have ADAPTIVE PLANNING section encouraging mid-execution revise_plan."
        )
        # Must mention constraints to prevent over-revision
        # 必须同时给出约束，防止 Lead 反复改计划
        assert "anti-spiral" in LEAD_SYSTEM_PROMPT.lower() or "CONSTRAINTS" in LEAD_SYSTEM_PROMPT, (
            "ADAPTIVE PLANNING section must include constraints to prevent over-revision."
        )

    def test_adaptive_planning_restricts_new_tasks(self):
        """A5: ADAPTIVE PLANNING must restrict new task creation to escalation-only after Phase 1."""
        from llm_service.roles.swarm.lead_protocol import LEAD_SYSTEM_PROMPT
        adaptive_section = LEAD_SYSTEM_PROMPT.split("ADAPTIVE PLANNING")[1].split("INITIAL PLANNING")[0]
        assert "do not create new tasks unless" in adaptive_section.lower(), \
            "ADAPTIVE PLANNING must restrict new task creation after Phase 1"
        assert "escalat" in adaptive_section.lower(), \
            "New tasks should only be created in response to agent escalation"


class TestModuleExports:
    """Tests for roles/swarm/__init__.py exports."""

    def test_all_exports_available(self):
        """All key symbols should be importable from roles.swarm."""
        from llm_service.roles.swarm import (
            AGENT_LOOP_SYSTEM_PROMPT,
            LEAD_SYSTEM_PROMPT,
            SWARM_ROLE_PROMPTS,
            get_swarm_role_catalog,
        )

        assert AGENT_LOOP_SYSTEM_PROMPT
        assert LEAD_SYSTEM_PROMPT
        assert SWARM_ROLE_PROMPTS
        assert callable(get_swarm_role_catalog)


class TestLeadFileReadAction:
    def test_lead_has_file_read_action(self):
        from llm_service.roles.swarm.lead_protocol import LEAD_SYSTEM_PROMPT
        assert "file_read" in LEAD_SYSTEM_PROMPT

    def test_quality_gate_references_file_content(self):
        from llm_service.roles.swarm.lead_protocol import LEAD_SYSTEM_PROMPT
        assert "file_read" in LEAD_SYSTEM_PROMPT and ("verify" in LEAD_SYSTEM_PROMPT.lower() or "File preview" in LEAD_SYSTEM_PROMPT)


class TestLeadAnalysisQualityGate:
    """Lead must verify analysis task deliverables: data file + summary."""

    def test_analysis_quality_gate_checks_dual_deliverables(self):
        """Lead quality gate for analysis tasks must check both data and MD files."""
        from llm_service.roles.swarm.lead_protocol import LEAD_SYSTEM_PROMPT
        quality_section = LEAD_SYSTEM_PROMPT.split("TASK-TYPE-SPECIFIC VERIFICATION")[1].split("ADAPTIVE PLANNING")[0]
        assert "data file" in quality_section.lower() or "CSV" in quality_section, \
            "Analysis quality gate must check for data file (CSV/JSON)"
        assert "MD" in quality_section or "summary" in quality_section.lower(), \
            "Analysis quality gate must check for summary file"

    def test_analysis_quality_gate_cross_agent_consistency(self):
        """Lead must compare deliverable types when 2+ parallel analysts complete."""
        from llm_service.roles.swarm.lead_protocol import LEAD_SYSTEM_PROMPT
        quality_section = LEAD_SYSTEM_PROMPT.split("TASK-TYPE-SPECIFIC VERIFICATION")[1].split("ADAPTIVE PLANNING")[0]
        assert "parallel" in quality_section.lower() or "2+" in quality_section, \
            "Analysis quality gate must address cross-agent deliverable consistency"

    def test_task_description_requires_deliverables_for_analysis(self):
        """Lead task description spec must require explicit deliverables for analysis tasks."""
        from llm_service.roles.swarm.lead_protocol import LEAD_SYSTEM_PROMPT
        task_desc_section = LEAD_SYSTEM_PROMPT.split("TASK DESCRIPTIONS")[1].split("GOOD example")[0]
        assert "ANALYSIS" in task_desc_section or "deliverable" in task_desc_section.lower(), \
            "Task description spec must require explicit deliverables for analysis tasks"


class TestAgentQualitySelfCheck:
    """Agent protocol must have quality self-check before going idle.

    agent 进 idle 前先自查交付质量，避免半成品直接回流给 Lead。
    """

    def test_phase3_has_quality_check(self):
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert "QUALITY SELF-CHECK" in AGENT_LOOP_SYSTEM_PROMPT

    def test_idle_example_has_key_findings(self):
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert '"key_findings"' in AGENT_LOOP_SYSTEM_PROMPT

    def test_no_go_deep_instruction(self):
        """A1: 'Go DEEP' causes infinite search loops. Must be replaced with quantified limit.

        「Go DEEP」这类无上限措辞会让 agent 无限搜索，必须换成明确的数量上限。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        prompt = AGENT_LOOP_SYSTEM_PROMPT
        assert "Go DEEP" not in prompt, "AGENT_LOOP_SYSTEM_PROMPT must not contain 'Go DEEP' — causes infinite search loops"
        assert "don't stop after 2-3 tool calls" not in prompt, "Must not discourage early stopping"

    def test_has_tool_call_budget(self):
        """A1: PHASE 2 must specify a numeric tool call budget per task."""
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        prompt = AGENT_LOOP_SYSTEM_PROMPT
        import re
        assert re.search(r'\d+-\d+ tool call', prompt), "PHASE 2 must specify numeric tool call budget (e.g. '5-8 tool calls')"

    def test_quality_selfcheck_no_search_trigger(self):
        """A2: QUALITY SELF-CHECK must NOT trigger additional searches — causes N+1 loop.

        自查里再触发搜索会变成 N+1 循环：每次自查都引出新搜索，永远收不了尾。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        prompt = AGENT_LOOP_SYSTEM_PROMPT
        assert "do one more search" not in prompt, "QUALITY SELF-CHECK must not trigger more searches — causes N+1 loop"

    def test_continue_protocol_has_call_limit(self):
        """A4: CONTINUE PROTOCOL must limit tool calls to prevent runaway on reassignment.

        task 被重新指派后 agent 会接着干，没有调用上限就会一路跑飞。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        prompt = AGENT_LOOP_SYSTEM_PROMPT
        continue_section = prompt.split("CONTINUE PROTOCOL")[1].split("PHASE 1 — ORIENT")[0]
        import re
        assert re.search(r'\d+-\d+ tool call', continue_section), \
            "CONTINUE PROTOCOL must specify numeric tool call budget"

    def test_phase2_has_focus_constraint(self):
        """C2: PHASE 2 must instruct agents to stay on their assigned topic.

        要求 agent 只做分到的话题，避免越界去搜别的域、和同伴重复劳动。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        prompt = AGENT_LOOP_SYSTEM_PROMPT
        # Extract PHASE 2 section: from "PHASE 2 — EXECUTE" to "PHASE 3 — SAVE"
        # 只截取 PHASE 2 区段再检查，避免命中其它阶段的同名措辞
        phase2 = prompt.split("PHASE 2 — EXECUTE")[1].split("PHASE 3 — SAVE")[0]
        assert "STAY ON TOPIC" in phase2, \
            "PHASE 2 must include STAY ON TOPIC constraint to prevent cross-domain searching"


class TestDateInjection:
    """Task 0: Verify current date is injected into swarm prompts.

    当前日期需注入 prompt，否则模型会把「今年」「最新」算成训练数据里的年份。
    """

    def test_lead_prompt_has_date(self):
        """Lead user prompt must include current date for temporal awareness."""
        import inspect

        from llm_service.api.lead import _build_lead_user_prompt
        source = inspect.getsource(_build_lead_user_prompt)
        assert "current_date" in source, \
            "_build_lead_user_prompt must inject current_date into user prompt"
        assert "datetime" in source, \
            "_build_lead_user_prompt must use datetime for current date"

    def test_agent_loop_prompt_has_date(self):
        """Agent loop system prompt must include current date prefix."""
        import inspect

        from llm_service.api.agent import build_agent_messages
        source = inspect.getsource(build_agent_messages)
        assert "current_date" in source, \
            "build_agent_messages must inject current_date into system prompt"


class TestAgentBatchFetch:
    """Agent protocol must encourage batch web_fetch to reduce iteration count.

    鼓励一次 web_fetch 传多个 URL：串行抓取会让每个 agent 多烧几轮 LLM 调用。
    """

    def test_has_batch_fetch_example(self):
        """Agent protocol must have a web_fetch example using urls=[] batch mode."""
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert '"urls"' in AGENT_LOOP_SYSTEM_PROMPT, (
            "Agent protocol must have a web_fetch example with urls=[] batch mode. "
            "Without a few-shot example, LLMs never discover the batch parameter."
        )

    def test_has_batch_fetch_guidance(self):
        """TOOL BEST PRACTICES must guide agents to use batch fetch."""
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert "urls=" in AGENT_LOOP_SYSTEM_PROMPT or "urls=[" in AGENT_LOOP_SYSTEM_PROMPT, (
            "TOOL BEST PRACTICES must mention urls=[] batch mode for web_fetch."
        )

    def test_discourages_serial_fetch(self):
        """Protocol should discourage multiple sequential web_fetch calls for different URLs."""
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert "NOT multiple" in AGENT_LOOP_SYSTEM_PROMPT and "web_fetch" in AGENT_LOOP_SYSTEM_PROMPT, (
            "Protocol must discourage multiple sequential web_fetch calls."
        )


class TestAgentPhaseOptimization:
    """Agent protocol must optimize phase transitions to reduce wasted iterations.

    精简阶段切换里可以跳过的步骤，每个 agent 每个步骤都是一次额外的 LLM 调用。
    """

    def test_phase1_has_empty_workspace_skip(self):
        """Phase 1 should instruct agents to skip orient on empty workspace."""
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        phase1 = AGENT_LOOP_SYSTEM_PROMPT.split("PHASE 1")[1].split("TOOL SELECTION")[0]
        assert "skip" in phase1.lower() or "SKIP" in phase1, (
            "Phase 1 must have guidance to skip orient when workspace is empty and no tasks are completed."
        )
        assert "no completed tasks" in phase1.lower() or "empty" in phase1.lower(), (
            "Phase 1 skip condition must reference empty workspace or no completed tasks."
        )

    def test_phase3_no_mandatory_file_list(self):
        """Phase 3 should NOT require file_list before idle — agent already knows workspace from notes.

        notes 里已经有 workspace 信息，再列一次目录只是白烧一次 LLM 调用。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        phase3 = AGENT_LOOP_SYSTEM_PROMPT.split("PHASE 3 — SAVE")[1].split("QUALITY SELF-CHECK")[0]
        assert 'file_list(".")' not in phase3, (
            "Phase 3 must NOT require file_list('.') — agent already has workspace knowledge from notes. "
            "This wastes 1 LLM call per agent."
        )

    def test_publish_data_not_too_frequent(self):
        """publish_data should NOT be on a fixed schedule — agent decides when findings are worth sharing.

        固定节奏 publish 只是例行状态汇报，每次都要多烧 3-5 次 LLM 调用；
        应由 agent 判断手上结果是否值得共享。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert "every 2-3 tool_calls" not in AGENT_LOOP_SYSTEM_PROMPT, (
            "publish_data every 2-3 tool calls wastes 3-5 LLM calls per agent on routine status. "
            "Agent should publish when it has findings worth sharing, not on a fixed schedule."
        )
        assert "every 2-3 iterations" not in AGENT_LOOP_SYSTEM_PROMPT, (
            "Same issue — publish_data frequency should not be on a fixed schedule."
        )
