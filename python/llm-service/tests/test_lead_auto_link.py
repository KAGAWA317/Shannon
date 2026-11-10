"""Tests for _best_match in lead.py (_auto_link_task_ids helper).

Covers:
- Punctuation stripping via regex (dot, comma, etc. no longer block matches)
- 60% threshold rejects short descriptions with only 50% overlap
- Exact substring match still returns immediately

lead.py 中 _best_match（_auto_link_task_ids 辅助函数）的测试。

覆盖：
- 通过正则剥离标点（点号、逗号等不再阻碍匹配）
- 60% 阈值会拒绝仅 50% 重叠的短描述
- 精确子串匹配仍然立即返回
"""

from llm_service.api.lead import _best_match


class TestPunctuationStripping:
    """Verify that punctuation in descriptions does not break word matching.
    验证描述中的标点不会破坏单词匹配。
    """

    def test_punctuation_does_not_break_matching(self):
        """'pricing.' in task_desc should still match 'pricing' in spawn_desc.
        task_desc 中的 'pricing.' 仍应匹配 spawn_desc 中的 'pricing'。
        """
        available = {"T1": "Analyze competitor pricing. Include all tiers."}
        claimed: set = set()
        # spawn_desc uses the word without trailing period
        # spawn_desc 使用的词没有结尾句点
        spawn_desc = "Analyze competitor pricing and tiers for the report"
        result = _best_match(spawn_desc, available, claimed)
        assert result == "T1", (
            f"Expected T1 but got '{result}'. "
            "Punctuation in task_desc should not block word matching."
        )

    def test_comma_in_description_handled(self):
        """Commas within description words should be stripped correctly.
        描述单词内的逗号应被正确剥离。
        """
        available = {"T2": "Research pricing, features, and availability"}
        claimed: set = set()
        spawn_desc = "Research pricing features and availability in the market"
        result = _best_match(spawn_desc, available, claimed)
        assert result == "T2"

    def test_old_split_would_fail(self):
        """Demonstrate that the old .split() approach would fail on punctuation.

        With .split(), 'pricing.' != 'pricing', so overlaps are undercounted.
        The new regex approach extracts 'pricing' from 'pricing.' correctly.

        演示旧的 .split() 方案在标点上会失败。

        用 .split() 时 'pricing.' != 'pricing'，词重叠会被少算。
        新的正则方案能正确地从 'pricing.' 中提取出 'pricing'。
        """
        import re
        # Old behavior simulation
        # 旧行为模拟
        task_with_period = "pricing."
        spawn_word = "pricing"
        old_match = task_with_period in spawn_word.split()
        # 'pricing.' is NOT in ['pricing'] — this was the bug
        # 'pricing.' 不在 ['pricing'] 中 —— 这就是原来的 bug
        assert not old_match, "Confirms the old split() approach fails on punctuation"

        # New behavior
        # 新行为
        task_words = set(re.findall(r'\w+', task_with_period.lower()))
        spawn_words = set(re.findall(r'\w+', spawn_word.lower()))
        new_match = bool(task_words & spawn_words)
        assert new_match, "New regex approach correctly handles 'pricing.' == 'pricing'"


class TestThresholdTightening:
    """Verify that 60% threshold rejects marginal matches that 50% would accept.
    验证 60% 阈值会拒绝 50% 阈值会放过的边缘匹配。
    """

    def test_short_description_needs_high_overlap(self):
        """1 out of 2 words = 50% overlap → should NOT match at 60% threshold.
        2 个词中命中 1 个 = 50% 重叠 → 在 60% 阈值下不应匹配。
        """
        # task_desc has 2 words: "analyze pricing"
        # task_desc 有 2 个词："analyze pricing"
        # spawn_desc shares only 1 word: "analyze" (not "pricing")
        # spawn_desc 只共享 1 个词："analyze"（没有 "pricing"）
        available = {"T1": "analyze pricing"}
        claimed: set = set()
        spawn_desc = "analyze competitors thoroughly"
        result = _best_match(spawn_desc, available, claimed)
        # overlap = {"analyze"} → 1/2 = 50% < 60% threshold → no match
        # overlap = {"analyze"} → 1/2 = 50% < 60% 阈值 → 不匹配
        assert result == "", (
            f"Expected no match (50% < 60% threshold) but got '{result}'"
        )

    def test_60_percent_match_succeeds(self):
        """3 out of 5 words = 60% overlap → should match at 60% threshold.
        5 个词中命中 3 个 = 60% 重叠 → 在 60% 阈值下应匹配。
        """
        available = {"T1": "analyze competitor pricing strategy thoroughly"}
        claimed: set = set()
        # Shares: analyze, competitor, pricing (3/5 = 60%)
        # 共享：analyze、competitor、pricing（3/5 = 60%）
        spawn_desc = "analyze competitor pricing the market broadly"
        result = _best_match(spawn_desc, available, claimed)
        assert result == "T1", (
            f"Expected T1 (60% overlap) but got '{result}'"
        )

    def test_old_50_threshold_would_accept_ambiguous_match(self):
        """Confirm that the old 50% threshold would accept what 60% now rejects.
        确认旧的 50% 阈值会接受现在 60% 拒绝的匹配。
        """
        # 1/2 = 50% — old code accepted this, new code rejects it
        # 1/2 = 50% —— 旧代码接受，新代码拒绝
        task_words = {"analyze", "pricing"}
        spawn_words = {"analyze", "competitors", "thoroughly"}
        overlap = len(spawn_words & task_words)
        score = overlap / len(task_words)
        assert score == 0.5, f"Expected score 0.5, got {score}"
        # Old threshold: 0.5 >= 0.5 → True (false positive)
        # 旧阈值：0.5 >= 0.5 → True（误匹配）
        assert score >= 0.5, "Old 50% threshold would accept this"
        # New threshold: 0.5 >= 0.6 → False (correctly rejected)
        # 新阈值：0.5 >= 0.6 → False（正确拒绝）
        assert not (score >= 0.6), "New 60% threshold correctly rejects this"


class TestExactSubstringMatch:
    """Verify exact substring matching still returns immediately.
    验证精确子串匹配仍会立即返回。
    """

    def test_exact_substring_still_works(self):
        """When spawn_desc contains the full task_desc as substring, return immediately.
        当 spawn_desc 以子串形式包含完整 task_desc 时，立即返回。
        """
        available = {
            "T1": "analyze competitor pricing",
            "T2": "write synthesis report",
        }
        claimed: set = set()
        # spawn_desc contains T1's description verbatim
        # spawn_desc 原样包含 T1 的描述
        spawn_desc = "analyze competitor pricing for all major vendors"
        result = _best_match(spawn_desc, available, claimed)
        assert result == "T1", (
            f"Expected T1 via exact substring match, got '{result}'"
        )

    def test_exact_match_skips_claimed(self):
        """A claimed task should not be returned even on exact match.
        即使精确匹配，已被认领的 task 也不应被返回。
        """
        available = {"T1": "analyze competitor pricing"}
        claimed = {"T1"}
        spawn_desc = "analyze competitor pricing"
        result = _best_match(spawn_desc, available, claimed)
        assert result == "", "Claimed task should not be returned"

    def test_exact_match_takes_priority_over_word_overlap(self):
        """Exact substring match should beat higher word-overlap candidates.
        精确子串匹配应胜过词重叠更高的候选。
        """
        available = {
            "T1": "research pricing",          # 2/2 = 100% word overlap
            # 2/2 = 100% 词重叠
            "T2": "analyze competitor pricing strategy",  # exact substring match
            # 精确子串匹配
        }
        claimed: set = set()
        # spawn_desc contains T2 verbatim as prefix — exact match should win
        # spawn_desc 前缀原样包含 T2 —— 精确匹配应胜出
        spawn_desc = "analyze competitor pricing strategy in detail"
        result = _best_match(spawn_desc, available, claimed)
        assert result == "T2", (
            f"Expected T2 (exact substring) but got '{result}'"
        )

    def test_no_match_returns_empty_string(self):
        """When no task meets the threshold, return empty string.
        没有 task 达到阈值时，返回空字符串。
        """
        available = {"T1": "completely unrelated task description here"}
        claimed: set = set()
        spawn_desc = "analyze pricing"
        result = _best_match(spawn_desc, available, claimed)
        assert result == ""
