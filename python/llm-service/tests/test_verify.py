"""Tests for verify.py claim verification utilities.

覆盖 verify.py 的 claim 校验 tool：语言检测、分词、BM25 打分、citation 编号提取，
以及 Pydantic 模型的默认值。
"""

import pytest
from llm_service.api.verify import (
    detect_language,
    tokenize,
    bm25_score,
    _extract_cited_numbers,
    Citation,
    ClaimVerification,
    VerificationResult,
)


class TestDetectLanguage:
    """Tests for language detection."""

    def test_english_text(self):
        assert detect_language("This is an English sentence.") == "en"

    def test_chinese_text(self):
        assert detect_language("这是一个中文句子。") == "zh"

    def test_japanese_text(self):
        # Japanese is grouped with CJK, returns "zh"
        # 日文同属 CJK 字符集，检测结果归为 "zh"
        assert detect_language("これは日本語の文です。") == "zh"

    def test_mixed_mostly_english(self):
        assert detect_language("Hello world 你好") == "en"

    def test_mixed_mostly_chinese(self):
        assert detect_language("你好世界 hello") == "zh"

    def test_empty_string(self):
        assert detect_language("") == "en"  # Default fallback 注：默认兜底


class TestTokenize:
    """Tests for tokenization."""

    def test_english_tokenization(self):
        tokens = tokenize("The quick brown fox")
        # Note: tokenize does NOT filter stopwords, just tokenizes
        # 注意：tokenize 不过滤 stopword，只做分词
        assert "quick" in tokens
        assert "brown" in tokens
        assert "fox" in tokens
        assert "the" in tokens  # Stopwords are included (lowercase) 注：停用词会被保留（小写）

    def test_chinese_tokenization(self):
        tokens = tokenize("人工智能技术")
        # Character-based tokenization for CJK
        # CJK 按单字切分
        assert len(tokens) > 0
        assert "人" in tokens
        assert "工" in tokens

    def test_mixed_tokenization(self):
        tokens = tokenize("AI人工智能 technology")
        assert "technology" in tokens
        # CJK characters are extracted individually
        # CJK 字符逐个提取，英文仍按词切
        assert "人" in tokens
        assert len(tokens) > 1

    def test_empty_string(self):
        tokens = tokenize("")
        assert tokens == []


class TestBM25Score:
    """Tests for BM25 scoring."""

    def test_exact_match(self):
        query_tokens = ["revenue", "growth"]
        doc_tokens = ["revenue", "growth", "2024"]
        score = bm25_score(query_tokens, doc_tokens)
        assert score > 0

    def test_no_match(self):
        query_tokens = ["revenue", "growth"]
        doc_tokens = ["weather", "forecast"]
        score = bm25_score(query_tokens, doc_tokens)
        assert score == 0

    def test_partial_match(self):
        query_tokens = ["revenue", "growth"]
        doc_tokens = ["revenue", "decline", "2024"]
        score = bm25_score(query_tokens, doc_tokens)
        assert score > 0

    def test_empty_query(self):
        query_tokens = []
        doc_tokens = ["revenue", "growth"]
        score = bm25_score(query_tokens, doc_tokens)
        assert score == 0

    def test_empty_doc(self):
        query_tokens = ["revenue", "growth"]
        doc_tokens = []
        score = bm25_score(query_tokens, doc_tokens)
        assert score == 0


class TestExtractCitedNumbers:
    """Tests for citation number extraction."""

    def test_single_citation(self):
        text = "Revenue grew 19%[1]."
        result = _extract_cited_numbers(text, max_citations=10)
        assert 1 in result

    def test_multiple_citations(self):
        text = "Revenue grew 19%[1][2]. Profit increased[3]."
        result = _extract_cited_numbers(text, max_citations=10)
        assert {1, 2, 3} == result

    def test_no_citations(self):
        text = "Revenue grew 19%."
        result = _extract_cited_numbers(text, max_citations=10)
        assert result == set()

    def test_respects_max_citations(self):
        text = "Fact[1]. Another[2]. More[15]."
        result = _extract_cited_numbers(text, max_citations=10)
        assert 1 in result
        assert 2 in result
        assert 15 not in result  # Exceeds max_citations 注：超出 max_citations

    def test_limit_parameter(self):
        text = "[1][2][3][4][5][6][7][8][9][10]"
        result = _extract_cited_numbers(text, max_citations=100, limit=5)
        assert len(result) == 5


class TestModels:
    """Tests for Pydantic models."""

    def test_citation_model_defaults(self):
        citation = Citation(url="https://example.com")
        assert citation.title == ""
        assert citation.credibility_score == 0.5
        assert citation.content is None

    def test_citation_model_extra_fields_ignored(self):
        # Should not raise even with extra fields (extra="ignore")
        # 多余字段被忽略（extra="ignore"），不应抛 exception
        citation = Citation(url="https://example.com", unknown_field="test")
        assert citation.url == "https://example.com"

    def test_claim_verification_defaults(self):
        cv = ClaimVerification(claim="Test claim")
        assert cv.supporting_citations == []
        assert cv.confidence == 0.0

    def test_verification_result_defaults(self):
        result = VerificationResult(
            overall_confidence=0.8,
            total_claims=10,
            supported_claims=8,
        )
        assert result.unsupported_claims == []
        assert result.conflicts == []
