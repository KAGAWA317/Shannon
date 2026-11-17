"""Claim verification API for cross-referencing synthesis against citations.

对照 citations 交叉核对 synthesis 的 claim 验证 API。
"""

import json
import logging
import math
import re
from collections import Counter
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from pydantic.config import ConfigDict

logger = logging.getLogger(__name__)

# CJK character pattern for language detection and tokenization
# 用于语言检测与分词的 CJK 字符模式
CJK_PATTERN = re.compile(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]')

router = APIRouter()


class Citation(BaseModel):
    """Citation structure matching Go orchestrator.

    与 Go orchestrator 匹配的 citation 结构。
    """
    model_config = ConfigDict(extra="ignore")

    url: str = ""
    title: str = ""
    source: str = ""
    content: str | None = None
    snippet: str | None = None
    credibility_score: float = 0.5
    quality_score: float = 0.5


class ClaimVerification(BaseModel):
    """Verification result for a single claim.

    单个 claim 的验证结果。
    """
    claim: str
    supporting_citations: list[int] = Field(default_factory=list)
    conflicting_citations: list[int] = Field(default_factory=list)
    confidence: float = 0.0


class ConflictReport(BaseModel):
    """Report of conflicting information between sources.

    sources 之间冲突信息的报告。
    """
    claim: str
    source1: int
    source1_text: str
    source2: int
    source2_text: str


class VerificationResult(BaseModel):
    """Overall verification result.

    整体验证结果。
    """
    overall_confidence: float
    total_claims: int
    supported_claims: int
    unsupported_claims: list[str] = Field(default_factory=list)
    conflicts: list[ConflictReport] = Field(default_factory=list)
    claim_details: list[ClaimVerification] = Field(default_factory=list)


# ============================================================================
# V2 Models with three-category classification
# 带 three-category 分类的 V2 Models
# ============================================================================

class ClaimVerificationV2(BaseModel):
    """V2 Verification result with three-category classification.

    带 three-category 分类的 V2 验证结果。
    """
    claim: str
    verdict: str = "insufficient_evidence"  # "supported" | "unsupported" | "insufficient_evidence" 注：枚举取值 supported / unsupported / insufficient_evidence
    supporting_citations: list[int] = Field(default_factory=list)
    conflicting_citations: list[int] = Field(default_factory=list)
    confidence: float = 0.0
    retrieval_scores: dict[int, float] = Field(default_factory=dict)  # citation_id → relevance 注：citation_id → 相关性
    reasoning: str = ""


class VerificationResultV2(BaseModel):
    """V2 Overall verification result with three-category breakdown.

    带 three-category 分类统计的 V2 整体验证结果。
    """
    overall_confidence: float
    total_claims: int

    # Three-category counts
    # three-category 计数
    supported_claims: int
    unsupported_claims: int
    insufficient_evidence_claims: int

    # Lists for each category
    # 各类别的列表
    supported_claim_texts: list[str] = Field(default_factory=list)
    unsupported_claim_texts: list[str] = Field(default_factory=list)
    insufficient_claim_texts: list[str] = Field(default_factory=list)

    # Details
    # 明细
    claim_details: list[ClaimVerificationV2] = Field(default_factory=list)
    conflicts: list[ConflictReport] = Field(default_factory=list)

    # Quality metrics
    # 质量 metrics
    evidence_coverage: float = 0.0  # % of claims with relevant citations found 注：找到相关 citations 的 claim 占比
    avg_retrieval_score: float = 0.0  # Average top-1 retrieval relevance 注：top-1 检索相关性均值


# ============================================================================
# V2 Helper Functions: Language Detection & BM25 Retrieval
# V2 辅助函数：语言检测与 BM25 检索
# ============================================================================

def detect_language(text: str) -> str:
    """
    Detect if text is primarily CJK (Chinese/Japanese/Korean) or Latin-based.

    检测文本主要是 CJK（中文/日文/韩文）还是拉丁语系。

    Args:
        text: Text to analyze (first ~500 chars recommended)
            待分析的文本（建议取前约 500 字符）

    Returns:
        "zh" for CJK-dominant text, "en" otherwise
        CJK 为主的文本返回 "zh"，否则返回 "en"
    """
    if not text:
        return "en"

    # Count CJK characters vs total alphanumeric
    # 统计 CJK 字符数与字母数字总数
    cjk_count = len(CJK_PATTERN.findall(text))
    # Count Latin letters
    # 统计拉丁字母数
    latin_count = len(re.findall(r'[a-zA-Z]', text))

    total = cjk_count + latin_count
    if total == 0:
        return "en"

    # If >30% CJK, treat as CJK-dominant
    # 若 CJK 占比 >30%，视为 CJK 为主
    if cjk_count / total > 0.3:
        return "zh"
    return "en"


def tokenize(text: str) -> list[str]:
    """
    Tokenize text supporting both CJK (character-level) and Latin (word-level).

    对文本分词，同时支持 CJK（字符级）与拉丁语系（词级）。

    Args:
        text: Text to tokenize
            待分词的文本

    Returns:
        List of tokens (CJK chars + Latin words)
        token 列表（CJK 字符 + 拉丁词）
    """
    if not text:
        return []

    tokens = []

    # CJK: character-level tokenization
    # CJK：字符级分词
    for char in text:
        if CJK_PATTERN.match(char):
            tokens.append(char)

    # Latin: word-level tokenization (lowercase)
    # 拉丁语系：词级分词（转为小写）
    words = re.findall(r'\b\w+\b', text.lower())
    tokens.extend(words)

    return tokens


class CorpusStats:
    """P1-5: Corpus statistics for IDF calculation.

    P1-5：用于 IDF 计算的语料统计。
    """
    def __init__(self):
        self.doc_freq: dict[str, int] = {}  # term → number of documents containing it 注：term → 包含该 term 的 document 数
        self.total_docs: int = 0
        self.avg_doc_len: float = 200.0

    @classmethod
    def from_citations(cls, citations: list["Citation"]) -> "CorpusStats":
        """Compute corpus statistics from a list of citations.

        从 citation 列表计算语料统计。
        """
        stats = cls()
        stats.total_docs = len(citations)

        if not citations:
            return stats

        total_tokens = 0
        for c in citations:
            text = f"{c.title or ''} {c.content or c.snippet or ''}"
            tokens = tokenize(text)
            total_tokens += len(tokens)

            # Count document frequency (each term counted once per document)
            # 统计 document frequency（每个 term 在每个 document 中只计一次）
            unique_terms = set(tokens)
            for term in unique_terms:
                stats.doc_freq[term] = stats.doc_freq.get(term, 0) + 1

        if stats.total_docs > 0:
            stats.avg_doc_len = total_tokens / stats.total_docs
            if stats.avg_doc_len <= 0:
                stats.avg_doc_len = 200.0

        return stats


def bm25_score(
    query_tokens: list[str],
    doc_tokens: list[str],
    k1: float = 1.5,
    b: float = 0.75,
    avg_doc_len: float = 200.0,
    corpus_stats: CorpusStats | None = None
) -> float:
    """
    Compute BM25 score between query and document.

    计算 query 与 document 之间的 BM25 得分。

    P1-5: Now includes IDF weighting when corpus_stats is provided.

    P1-5：提供 corpus_stats 时，现包含 IDF 加权。

    Args:
        query_tokens: Tokenized query (claim)
            分词后的 query（claim）
        doc_tokens: Tokenized document (citation content)
            分词后的 document（citation 内容）
        k1: Term frequency saturation parameter
            词频饱和参数
        b: Length normalization parameter
            长度归一化参数
        avg_doc_len: Estimated average document length (used if corpus_stats not provided)
            估算的平均 document 长度（未提供 corpus_stats 时使用）
        corpus_stats: Optional corpus statistics for IDF calculation
            可选的语料统计，用于 IDF 计算

    Returns:
        BM25 relevance score (higher = more relevant)
        BM25 相关性得分（越高越相关）
    """
    if not query_tokens or not doc_tokens:
        return 0.0

    doc_freq = Counter(doc_tokens)
    doc_len = len(doc_tokens)

    # Use corpus stats if available
    # 若可用则使用 corpus stats
    if corpus_stats and corpus_stats.total_docs > 0:
        avg_doc_len = corpus_stats.avg_doc_len
        N = corpus_stats.total_docs
    else:
        N = 0  # No IDF if no corpus stats 注：无 corpus stats 时不做 IDF 加权
    if avg_doc_len <= 0:
        avg_doc_len = 200.0

    score = 0.0
    for term in set(query_tokens):
        tf = doc_freq.get(term, 0)
        if tf > 0:
            # TF component (BM25 saturation)
            # TF 分量（BM25 饱和）
            tf_component = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / avg_doc_len))

            # IDF component (P1-5: proper inverse document frequency)
            # IDF 分量（P1-5：规范的 inverse document frequency）
            if N > 0 and corpus_stats:
                n = corpus_stats.doc_freq.get(term, 0)
                # BM25 IDF formula: log((N - n + 0.5) / (n + 0.5) + 1)
                # The +1 inside log prevents negative scores for very common terms
                # BM25 IDF 公式：log((N - n + 0.5) / (n + 0.5) + 1)
                # log 内的 +1 可避免常见 term 得到负分
                idf = math.log((N - n + 0.5) / (n + 0.5) + 1)
            else:
                idf = 1.0  # No IDF weighting without corpus stats 注：无 corpus stats 时不做 IDF 加权

            score += idf * tf_component

    return score


def retrieve_relevant_citations(
    claim: str,
    citations: list[Citation],
    top_k: int = 5,
    corpus_stats: CorpusStats | None = None
) -> list[tuple[int, Citation, float]]:
    """
    Retrieve top-k most relevant citations for a claim using BM25 scoring.

    使用 BM25 打分为 claim 检索最相关的前 k 个 citations。

    P1-5: Now uses IDF weighting via corpus_stats for better precision.

    P1-5：现通过 corpus_stats 使用 IDF 加权，以获得更好的精度。

    Args:
        claim: The claim to find evidence for
            需要寻找证据的 claim
        citations: List of all available citations
            所有可用 citations 的列表
        top_k: Number of top citations to return
            返回的 top citations 数量
        corpus_stats: Optional pre-computed corpus stats (computed if not provided)
            可选的预计算语料统计（未提供时即时计算）

    Returns:
        List of (original_1based_index, citation, relevance_score) tuples
        (original_1based_index, citation, relevance_score) 元组列表
    """
    if not citations or not claim:
        return []

    # Tokenize claim
    # 对 claim 分词
    claim_tokens = tokenize(claim)
    if not claim_tokens:
        return []

    # P1-5: Compute corpus stats if not provided
    # P1-5：未提供时计算 corpus stats
    if corpus_stats is None:
        corpus_stats = CorpusStats.from_citations(citations)

    # Score each citation
    # 为每个 citation 打分
    scored: list[tuple[int, Citation, float]] = []
    for idx, c in enumerate(citations):
        # Combine title + content/snippet for matching
        # 合并 title + content/snippet 用于匹配
        citation_text = f"{c.title or ''} {c.content or c.snippet or ''}"
        citation_tokens = tokenize(citation_text)

        # BM25 scoring with IDF
        # 带 IDF 的 BM25 打分
        score = bm25_score(claim_tokens, citation_tokens, corpus_stats=corpus_stats)

        # Boost by credibility (0.5 baseline + 0.5 * credibility)
        # 按 credibility 加权（0.5 基线 + 0.5 * credibility）
        score *= (0.5 + 0.5 * c.credibility_score)

        # 1-indexed for citation references
        # citation 引用使用 1-based 索引
        scored.append((idx + 1, c, score))

    # Sort by score descending, take top-k
    # 按得分降序排序，取 top-k
    scored.sort(key=lambda x: x[2], reverse=True)

    # Filter out zero-score citations
    # 过滤掉零分 citations
    scored = [(idx, c, s) for idx, c, s in scored if s > 0]

    return scored[:top_k]


def _extract_cited_numbers(text: str, max_citations: int, limit: int = 50) -> set:
    """
    Extract citation numbers referenced in text (e.g., [1], [37]).

    提取文本中引用的 citation 编号（例如 [1]、[37]）。

    Args:
        text: Text potentially containing citation references
            可能包含 citation 引用的文本
        max_citations: Maximum valid citation number (len(citations))
            最大有效 citation 编号（len(citations)）
        limit: Maximum number of unique citations to extract (prevent OOM)
            提取的 unique citations 上限（防止 OOM）

    Returns:
        Set of valid citation numbers found in text
        文本中找到的有效 citation 编号集合
    """
    cited_numbers = set()
    for match in re.findall(r'\[(\d+)\]', text):
        try:
            num = int(match)
            if 0 < num <= max_citations:
                cited_numbers.add(num)
            if len(cited_numbers) >= limit:
                break
        except ValueError:
            continue
    return cited_numbers


def _build_relevant_citations(
    citations: list["Citation"],
    cited_numbers: set,
    min_count: int = 10,
    max_count: int = 25
) -> list[tuple[int, "Citation"]]:
    """
    Build a list of relevant citations preserving original indices.

    构建保留原始索引的相关 citations 列表。

    Args:
        citations: Full list of Citation objects
            完整的 Citation 对象列表
        cited_numbers: Set of citation numbers actually referenced in answer
            answer 中实际引用的 citation 编号集合
        min_count: Minimum citations to include (pad with top-K if needed)
            最少包含的 citations 数量（不足时用 top-K 补齐）
        max_count: Maximum citations to include
            最多包含的 citations 数量

    Returns:
        List of (original_index, citation) tuples
        (original_index, citation) 元组列表
    """
    relevant = []

    # First, add all actually cited citations (preserving original index)
    # 首先加入所有实际被引用的 citations（保留原始索引）
    for num in sorted(cited_numbers):
        if num <= len(citations):
            relevant.append((num, citations[num - 1]))
        if len(relevant) >= max_count:
            break

    # If we have fewer than min_count, pad with top-K fallback
    # 若少于 min_count，则用 top-K fallback 补齐
    if len(relevant) < min_count:
        existing_nums = {num for num, _ in relevant}
        for i, c in enumerate(citations[:20]):  # Check first 20 注：只检查前 20 个
            idx = i + 1
            if idx not in existing_nums:
                relevant.append((idx, c))
                if len(relevant) >= min_count:
                    break

    return relevant


async def verify_claims(
    answer: str,
    citations: list[dict[str, Any]],
    llm_client: Any
) -> VerificationResult:
    """
    Verify factual claims in synthesis against collected citations.

    对照已收集的 citations 验证 synthesis 中的事实性 claims。

    Args:
        answer: Synthesis result containing claims
            包含 claims 的 synthesis 结果
        citations: List of citation dicts from orchestrator
            来自 orchestrator 的 citation dict 列表
        llm_client: LLM client for claim extraction
            用于 claim 提取的 LLM client

    Returns:
        VerificationResult with confidence scores and unsupported claims
        包含 confidence 得分与 unsupported claims 的 VerificationResult
    """

    # Parse citations (be tolerant of partial/mismatched fields)
    # 解析 citations（容忍字段缺失/不匹配）
    citation_objs: list[Citation] = []
    for idx, raw in enumerate(citations or []):
        try:
            citation_objs.append(Citation(**(raw or {})))
        except Exception as e:
            logger.warning(f"[verification] Failed to parse citation[{idx}]: {e}")

    # Extract which citation numbers are actually referenced in the answer
    # 提取 answer 中实际被引用的 citation 编号
    cited_numbers = _extract_cited_numbers(answer, len(citation_objs), limit=50)
    logger.debug(f"[verification] Found {len(cited_numbers)} unique citation references in answer: {sorted(cited_numbers)[:10]}...")

    # Build relevant citations with original indices preserved
    # 构建保留原始索引的相关 citations
    relevant_citations = _build_relevant_citations(citation_objs, cited_numbers, min_count=10, max_count=25)
    logger.debug(f"[verification] Using {len(relevant_citations)} relevant citations for verification")

    # Step 1: Extract factual claims using LLM
    # 步骤 1：使用 LLM 提取事实性 claims
    claims = await _extract_claims(answer, llm_client)
    logger.info(f"[verification] Extracted {len(claims)} claims from synthesis")

    if not claims:
        return VerificationResult(
            overall_confidence=1.0,  # No claims = nothing to verify 注：无 claims 即无需验证
            total_claims=0,
            supported_claims=0
        )

    # Step 2: Cross-reference each claim against citations (using relevant citations with original indices)
    # 步骤 2：将每个 claim 与 citations 交叉比对（使用带原始索引的相关 citations）
    claim_verifications = []
    for claim in claims:
        verification = await _verify_single_claim(claim, relevant_citations, llm_client)
        claim_verifications.append(verification)

    # Step 3: Calculate aggregate metrics
    # 步骤 3：计算聚合 metrics
    supported = sum(1 for cv in claim_verifications if cv.confidence >= 0.7)
    unsupported = [cv.claim for cv in claim_verifications if cv.confidence < 0.5]

    # Geometric mean: harsher on gaps than arithmetic
    # 几何平均：对覆盖缺口比算术平均更严格
    if claim_verifications:
        mean_conf = sum(cv.confidence for cv in claim_verifications) / len(claim_verifications)
        coverage = supported / max(1, len(claim_verifications))
        overall_conf = math.sqrt(max(0.0, min(1.0, mean_conf)) * max(0.0, min(1.0, coverage)))
    else:
        overall_conf = 1.0

    # Step 4: Detect conflicts (claims with both supporting AND conflicting citations)
    # 步骤 4：检测冲突（同时有 supporting 与 conflicting citations 的 claims）
    conflicts = _detect_conflicts(claim_verifications, citation_objs)

    logger.info(f"[verification] Overall confidence: {overall_conf:.2f}, " +
                f"Supported: {supported}/{len(claim_verifications)}, " +
                f"Unsupported: {len(unsupported)}, Conflicts: {len(conflicts)}")

    return VerificationResult(
        overall_confidence=overall_conf,
        total_claims=len(claims),
        supported_claims=supported,
        unsupported_claims=unsupported,
        conflicts=conflicts,
        claim_details=claim_verifications
    )


async def _extract_claims(answer: str, providers: Any) -> list[str]:
    """Extract factual claims from synthesis using LLM.

    使用 LLM 从 synthesis 中提取事实性 claims。
    """

    # P0 fix: Detect language and use appropriate prompt
    # This ensures claims are extracted correctly for CJK text
    # P0 修复：检测语言并使用对应的 prompt
    # 以确保 CJK 文本的 claims 能被正确提取
    lang = detect_language(answer[:500])

    if lang == "zh":
        prompt = f"""从以下文本中提取所有事实性陈述。
事实性陈述是指可以通过来源验证的声明。

文本:
{answer[:8000]}

指令:
1. 只提取事实性陈述（非观点或解释）
2. 每个陈述应是单一、可验证的声明
3. 以编号列表形式返回
4. 限制在最重要的 10 个陈述

输出格式:
1. [第一个陈述]
2. [第二个陈述]
...
"""
    else:
        prompt = f"""Extract all factual claims from the following text.
A factual claim is a statement that can be verified against sources.

Text:
{answer[:8000]}

Instructions:
1. Extract only factual claims (not opinions or interpretations)
2. Each claim should be a single, verifiable statement
3. Return as a numbered list
4. Limit to the 10 most important claims

Output format:
1. [First claim]
2. [Second claim]
...
"""

    try:
        # Use LLM to extract claims
        # 使用 LLM 提取 claims
        from llm_service.providers.base import ModelTier

        # max_tokens=8000: Claims extraction typically produces ~1500-2000 tokens
        # (10 claims × ~100-150 tokens each + JSON/list formatting overhead).
        # Previous value of 2000 caused truncation; 8000 provides 4x safety margin.
        # max_tokens=8000：claims 提取通常产出约 1500-2000 token
        # （10 个 claims × 每个约 100-150 token + JSON/list 格式化开销）。
        # 此前设为 2000 会导致截断；8000 提供了 4 倍安全余量。
        result = await providers.generate_completion(
            messages=[{"role": "user", "content": prompt}],
            tier=ModelTier.SMALL,
            max_tokens=8000,
            temperature=0.0,  # Deterministic extraction 注：确定性提取
            cache_source="verify_extract_claims",
        )

        response = result.get("output_text", "")

        # Parse numbered list
        # 解析编号列表
        claims = []
        for line in response.split('\n'):
            line = line.strip()
            if line and len(line) > 3:
                # Match patterns like "1. ", "1) ", or just starting with digit
                # 匹配 "1. "、"1) " 这类模式，或以数字开头
                if line[0].isdigit():
                    # Find the claim text after number and separator
                    # 在编号与分隔符之后取出 claim 文本
                    for sep in ['. ', ') ', ': ']:
                        if sep in line:
                            claim = line.split(sep, 1)[1].strip()
                            if claim:
                                claims.append(claim)
                            break

        logger.debug(f"[verification] Extracted {len(claims)} claims")
        return claims[:10]  # Limit to top 10 注：限制为前 10 个

    except Exception as e:
        logger.error(f"[verification] Failed to extract claims: {e}")
        return []


async def _verify_single_claim(
    claim: str,
    indexed_citations: list[tuple[int, Citation]],
    providers: Any
) -> ClaimVerification:
    """
    Verify a single claim against available citations.

    对照可用 citations 验证单个 claim。

    Args:
        claim: The factual claim to verify
            待验证的事实性 claim
        indexed_citations: List of (original_index, citation) tuples preserving original numbering
            保留原始编号的 (original_index, citation) 元组列表
        providers: LLM provider for verification
            用于验证的 LLM provider
    """

    if not indexed_citations:
        return ClaimVerification(claim=claim, confidence=0.0)

    # Build mapping from original index to citation for lookup
    # 建立 original index 到 citation 的映射以便查找
    idx_to_citation = {idx: c for idx, c in indexed_citations}

    # Build citation context using ORIGINAL indices (e.g., [1], [37], [42])
    # 使用原始索引构建 citation context（例如 [1]、[37]、[42]）
    citation_context = "\n\n".join([
        f"[{idx}] {(c.title or c.source or c.url)}\n{((c.content or c.snippet) or '')[:500]}"
        for idx, c in indexed_citations
    ])

    # List valid citation numbers for the prompt
    # 列出 prompt 中可用的 citation 编号
    valid_nums = sorted(idx_to_citation.keys())

    prompt = f"""Verify the following claim against the provided sources.

Claim: {claim}

Sources:
{citation_context}

For each source, determine if it:
- SUPPORTS the claim (provides evidence for it)
- CONFLICTS with the claim (contradicts it)
- NEUTRAL (doesn't address the claim)

Output JSON format:
{{
    "supporting": [{valid_nums[0]}],
    "conflicting": [],
    "confidence": 0.85
}}

IMPORTANT:
- Only use citation numbers from the sources above: {valid_nums}
- supporting/conflicting must be arrays of citation numbers
- confidence must be a number between 0.0 and 1.0
- Only output the JSON, nothing else.
"""

    try:
        # Use LLM for verification
        # 使用 LLM 进行验证
        from llm_service.providers.base import ModelTier

        result = await providers.generate_completion(
            messages=[{"role": "user", "content": prompt}],
            tier=ModelTier.SMALL,
            max_tokens=500,
            temperature=0.0,
            cache_source="verify_claim",
        )

        response = result.get("output_text", "")

        # Try to extract JSON from response
        # 尝试从 response 中提取 JSON
        response = response.strip()

        # Find JSON object in response
        # 在 response 中定位 JSON 对象
        json_start = response.find('{')
        json_end = response.rfind('}') + 1
        if json_start != -1 and json_end > json_start:
            json_str = response[json_start:json_end]
            result = json.loads(json_str)
        else:
            result = json.loads(response)

        supporting = result.get("supporting", [])
        conflicting = result.get("conflicting", [])
        base_confidence = result.get("confidence", 0.5)

        # Filter to only valid citation numbers and get credibility weights
        # 过滤出有效的 citation 编号并取得 credibility 权重
        valid_supporting = [n for n in supporting if n in idx_to_citation]
        valid_conflicting = [n for n in conflicting if n in idx_to_citation]

        # Weight confidence by citation credibility and count with diminishing returns (log2)
        # 按 citation credibility 加权 confidence，并对数量做递减（log2）处理
        if valid_supporting:
            credibility_weights = [idx_to_citation[n].credibility_score for n in valid_supporting]
            avg_cred = (sum(credibility_weights) / len(credibility_weights)) if credibility_weights else 0.5
            # Diminishing returns for multiple sources
            # 多个 sources 时收益递减
            num_sources = len(valid_supporting)
            bonus = min(0.25, 0.2 * math.log2(max(1, num_sources)))  # cap 25% 注：上限 25%
            confidence = base_confidence * avg_cred * (1.0 + bonus if num_sources > 1 else 1.0)
        else:
            confidence = 0.0

        # Weighted conflict penalty proportional to conflict strength
        # 按冲突强度比例加权的 conflict 惩罚
        if valid_conflicting:
            conflict_weight = sum(idx_to_citation[n].credibility_score for n in valid_conflicting)
            support_weight = sum(idx_to_citation[n].credibility_score for n in valid_supporting) if valid_supporting else 0.0
            denom = conflict_weight + support_weight
            if denom > 0:
                conflict_ratio = conflict_weight / denom
                penalty = 0.3 * conflict_ratio  # up to -30% 注：最高 -30%
            else:
                penalty = 0.2
            confidence *= (1.0 - penalty)

        # Clamp to [0,1]
        # 限制到 [0,1] 区间
        if confidence < 0:
            confidence = 0.0
        if confidence > 1:
            confidence = 1.0

        return ClaimVerification(
            claim=claim,
            supporting_citations=valid_supporting,
            conflicting_citations=valid_conflicting,
            confidence=confidence
        )

    except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
        logger.warning(f"[verification] Failed to parse LLM response for claim: {e}")
        # Fallback: assume moderate confidence if we can't verify
        # Fallback：无法验证时假定中等 confidence
        return ClaimVerification(claim=claim, confidence=0.5)
    except Exception as e:
        logger.error(f"[verification] Unexpected error verifying claim: {e}")
        return ClaimVerification(claim=claim, confidence=0.5)


def _detect_conflicts(
    verifications: list[ClaimVerification],
    citations: list[Citation]
) -> list[ConflictReport]:
    """Detect conflicting information across sources.

    检测 sources 之间冲突的信息。
    """

    conflicts = []
    for v in verifications:
        if v.supporting_citations and v.conflicting_citations:
            # This claim has both supporting AND conflicting citations
            # 该 claim 同时拥有 supporting 与 conflicting citations
            src1 = v.supporting_citations[0]
            src2 = v.conflicting_citations[0]

            if 0 < src1 <= len(citations) and 0 < src2 <= len(citations):
                conflicts.append(ConflictReport(
                    claim=v.claim,
                    source1=src1,
                    source1_text=citations[src1-1].title,
                    source2=src2,
                    source2_text=citations[src2-1].title
                ))

    return conflicts


# ============================================================================
# V2 Verification Functions (Three-category with BM25 retrieval)
# V2 验证函数（三类判定 + BM25 检索）
# ============================================================================

async def _verify_single_claim_v2(
    claim: str,
    all_citations: list[Citation],
    providers: Any,
    corpus_stats: CorpusStats | None = None
) -> ClaimVerificationV2:
    """
    Verify a single claim with evidence retrieval + three-category output.

    对单个 claim 做证据检索 + 三类判定输出。

    Uses BM25 to find relevant citations, then LLM judges:
    - supported: At least one source explicitly supports the claim
    - unsupported: A source explicitly contradicts the claim
    - insufficient_evidence: Sources don't directly address the claim

    先用 BM25 找出相关 citations，再由 LLM 判定：
    - supported：至少一个 source 明确支持该 claim
    - unsupported：有 source 明确反驳该 claim
    - insufficient_evidence：sources 未直接涉及该 claim

    P1-5: Accepts pre-computed corpus_stats for efficient IDF-weighted retrieval.

    P1-5：接受预计算的 corpus_stats，以实现高效的 IDF 加权检索。
    """

    # Step 1: Retrieve top-5 relevant citations via BM25 (with IDF)
    # 步骤 1：通过 BM25（带 IDF）检索前 5 个相关 citations
    relevant = retrieve_relevant_citations(claim, all_citations, top_k=5, corpus_stats=corpus_stats)

    if not relevant:
        return ClaimVerificationV2(
            claim=claim,
            verdict="insufficient_evidence",
            confidence=0.3,
            reasoning="No relevant citations found via retrieval"
        )

    # Step 2: Build context from relevant citations only
    # 步骤 2：仅用相关 citations 构建 context
    citation_context = "\n\n".join([
        f"[{idx}] (relevance: {score:.2f}) {c.title or c.source or c.url}\n{(c.content or c.snippet or '')[:500]}"
        for idx, c, score in relevant
    ])

    valid_nums = [idx for idx, _, _ in relevant]
    retrieval_scores = {idx: score for idx, _, score in relevant}

    # Step 3: Language-aware prompt
    # 步骤 3：语言感知的 prompt
    lang = detect_language(claim)

    if lang == "zh":
        prompt = f"""判断以下陈述是否被来源支持。

陈述: {claim}

相关来源 (按相关性排序):
{citation_context}

## 输出要求
只输出一个 JSON 对象（不要 Markdown/代码块），例如:
{{
    "verdict": "supported",
    "supporting": [{valid_nums[0]}],
    "conflicting": [],
    "confidence": 0.85,
    "reasoning": "简短解释"
}}

## 判定标准
- **supported**: 至少一个来源明确支持该陈述（有直接证据）
- **unsupported**: 有来源明确反驳该陈述（有矛盾证据）
- **insufficient_evidence**: 来源不直接涉及该陈述，或证据不足以判断

## 约束
- verdict 必须是 "supported"、"unsupported" 或 "insufficient_evidence"
- supporting/conflicting 只能包含以下 citation ID: {valid_nums}
- confidence 必须是 0.0 到 1.0 的数字

只使用以下 citation ID: {valid_nums}
只输出 JSON，无其他内容。
"""
    else:
        prompt = f"""Judge whether the following claim is supported by sources.

Claim: {claim}

Relevant sources (ranked by relevance):
{citation_context}

## Output format
{{
    "verdict": "supported",
    "supporting": [{valid_nums[0]}],
    "conflicting": [],
    "confidence": 0.85,
    "reasoning": "brief explanation"
}}

## Judgment criteria
- **supported**: At least one source explicitly supports the claim (direct evidence)
- **unsupported**: A source explicitly contradicts the claim (conflicting evidence)
- **insufficient_evidence**: Sources don't directly address the claim, or evidence is inconclusive

## Constraints
- verdict must be one of: "supported", "unsupported", "insufficient_evidence"
- supporting/conflicting must only include citation IDs from: {valid_nums}
- confidence must be a number between 0.0 and 1.0

Only use citation IDs: {valid_nums}
Output JSON only.
"""

    try:
        from llm_service.providers.base import ModelTier

        result = await providers.generate_completion(
            messages=[{"role": "user", "content": prompt}],
            tier=ModelTier.SMALL,
            max_tokens=500,
            temperature=0.0,
            cache_source="verify_claim_v2",
        )

        response = result.get("output_text", "").strip()

        # Parse JSON
        # 解析 JSON
        json_start = response.find('{')
        json_end = response.rfind('}') + 1
        if json_start != -1 and json_end > json_start:
            parsed = json.loads(response[json_start:json_end])
        else:
            parsed = json.loads(response)

        verdict = parsed.get("verdict", "insufficient_evidence")
        # Normalize verdict
        # 归一化 verdict
        if verdict not in ("supported", "unsupported", "insufficient_evidence"):
            verdict = "insufficient_evidence"

        supporting = [n for n in parsed.get("supporting", []) if n in valid_nums]
        conflicting = [n for n in parsed.get("conflicting", []) if n in valid_nums]
        base_confidence = float(parsed.get("confidence", 0.5))
        reasoning = parsed.get("reasoning", "")

        # Adjust confidence based on verdict and retrieval quality
        # 根据 verdict 与检索质量调整 confidence
        if verdict == "supported" and supporting:
            # Boost by retrieval score of best supporting citation
            # 按最佳 supporting citation 的检索得分加权
            max_retrieval = max(retrieval_scores.get(n, 0) for n in supporting)
            # Weighted: 60% LLM confidence + 40% retrieval quality (normalized)
            # 加权：60% LLM confidence + 40% 检索质量（归一化）
            confidence = 0.6 * base_confidence + 0.4 * min(1.0, max_retrieval / 5.0)
            # Ensure minimum confidence for supported claims
            # 保证 supported claims 的最低 confidence
            confidence = max(confidence, 0.5)
        elif verdict == "unsupported" and conflicting:
            # High confidence in contradiction
            # 明确矛盾时给予较高 confidence
            confidence = base_confidence * 0.9
            confidence = max(confidence, 0.6)
        else:
            # insufficient_evidence - moderate confidence
            # insufficient_evidence - 中等 confidence
            confidence = 0.3 + 0.2 * base_confidence

        # Clamp to [0, 1]
        # 限制到 [0, 1] 区间
        confidence = max(0.0, min(1.0, confidence))

        return ClaimVerificationV2(
            claim=claim,
            verdict=verdict,
            supporting_citations=supporting,
            conflicting_citations=conflicting,
            confidence=confidence,
            retrieval_scores=retrieval_scores,
            reasoning=reasoning
        )

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"[verification_v2] Failed to parse LLM response: {e}")
        return ClaimVerificationV2(
            claim=claim,
            verdict="insufficient_evidence",
            confidence=0.3,
            reasoning=f"Parse error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"[verification_v2] Unexpected error: {e}")
        return ClaimVerificationV2(
            claim=claim,
            verdict="insufficient_evidence",
            confidence=0.3,
            reasoning=f"Error: {str(e)}"
        )


async def verify_claims_v2(
    answer: str,
    citations: list[dict[str, Any]],
    llm_client: Any
) -> VerificationResultV2:
    """
    V2: Verify claims with BM25 evidence retrieval and three-category output.

    Improvements over V1:
    - BM25 retrieval finds relevant citations before LLM judgment
    - Three-category output: supported / unsupported / insufficient_evidence
    - Language-aware prompts for CJK and Latin text
    - Better confidence calibration

    Args:
        answer: Synthesis result containing claims
        citations: List of citation dicts from orchestrator
        llm_client: LLM client for claim extraction and verification

    Returns:
        VerificationResultV2 with three-category breakdown and quality metrics
    """

    # Parse citations
    # 解析 citations
    citation_objs: list[Citation] = []
    for idx, raw in enumerate(citations or []):
        try:
            citation_objs.append(Citation(**(raw or {})))
        except Exception as e:
            logger.warning(f"[verification_v2] Failed to parse citation[{idx}]: {e}")

    # Extract claims (reuse existing function)
    # 提取 claims（复用已有函数）
    claims = await _extract_claims(answer, llm_client)
    logger.info(f"[verification_v2] Extracted {len(claims)} claims from synthesis")

    if not claims:
        return VerificationResultV2(
            overall_confidence=1.0,
            total_claims=0,
            supported_claims=0,
            unsupported_claims=0,
            insufficient_evidence_claims=0,
            evidence_coverage=1.0,
            avg_retrieval_score=0.0
        )

    # P1-5: Pre-compute corpus stats once for all claims (IDF efficiency)
    # P1-5：为所有 claims 一次性预计算 corpus stats（提升 IDF 效率）
    corpus_stats = CorpusStats.from_citations(citation_objs)
    logger.debug(f"[verification_v2] Corpus stats: {corpus_stats.total_docs} docs, "
                 f"{len(corpus_stats.doc_freq)} unique terms, avg_len={corpus_stats.avg_doc_len:.1f}")

    # Verify each claim with V2 logic
    # 用 V2 逻辑验证每个 claim
    verifications: list[ClaimVerificationV2] = []
    for claim in claims:
        v = await _verify_single_claim_v2(claim, citation_objs, llm_client, corpus_stats=corpus_stats)
        verifications.append(v)

    # Aggregate by verdict
    # 按 verdict 汇总
    supported = sum(1 for v in verifications if v.verdict == "supported")
    unsupported = sum(1 for v in verifications if v.verdict == "unsupported")
    insufficient = sum(1 for v in verifications if v.verdict == "insufficient_evidence")

    # Collect claim texts by category
    # 按类别收集 claim 文本
    supported_texts = [v.claim for v in verifications if v.verdict == "supported"]
    unsupported_texts = [v.claim for v in verifications if v.verdict == "unsupported"]
    insufficient_texts = [v.claim for v in verifications if v.verdict == "insufficient_evidence"]

    # Calculate quality metrics
    # Evidence coverage: % of claims that got a definitive verdict (not insufficient)
    # 计算质量 metrics
    # 证据覆盖：得到确定性 verdict（非 insufficient）的 claim 占比
    evidence_coverage = (supported + unsupported) / len(verifications) if verifications else 0.0

    # Average top-1 retrieval score
    # top-1 检索得分均值
    retrieval_scores = []
    for v in verifications:
        if v.retrieval_scores:
            retrieval_scores.append(max(v.retrieval_scores.values()))
    avg_retrieval = sum(retrieval_scores) / len(retrieval_scores) if retrieval_scores else 0.0

    # Overall confidence (weighted by verdict type)
    # 整体 confidence（按 verdict 类型加权）
    if verifications:
        conf_scores = []
        for v in verifications:
            if v.verdict == "supported":
                conf_scores.append(v.confidence)
            elif v.verdict == "unsupported":
                # Unsupported claims reduce overall confidence
                # unsupported claims 会降低整体 confidence
                conf_scores.append(1.0 - v.confidence)
            else:
                # Insufficient evidence is neutral
                # 证据不足视为中性
                conf_scores.append(0.5)
        overall_conf = sum(conf_scores) / len(conf_scores)
    else:
        overall_conf = 1.0

    # Detect conflicts
    # 检测冲突
    conflicts = _detect_conflicts(verifications, citation_objs)

    logger.info(f"[verification_v2] Results: supported={supported}, unsupported={unsupported}, "
                f"insufficient={insufficient}, overall_conf={overall_conf:.2f}, "
                f"evidence_coverage={evidence_coverage:.2f}")

    return VerificationResultV2(
        overall_confidence=overall_conf,
        total_claims=len(claims),
        supported_claims=supported,
        unsupported_claims=unsupported,
        insufficient_evidence_claims=insufficient,
        supported_claim_texts=supported_texts,
        unsupported_claim_texts=unsupported_texts,
        insufficient_claim_texts=insufficient_texts,
        claim_details=verifications,
        conflicts=conflicts,
        evidence_coverage=evidence_coverage,
        avg_retrieval_score=avg_retrieval
    )


# ======================================================================
# FastAPI Endpoint
# FastAPI 端点
# ======================================================================

class VerifyClaimsRequest(BaseModel):
    """Request body for claim verification endpoint."""
    answer: str
    citations: list[dict[str, Any]]
    use_v2: bool = True  # Default to V2 注：默认使用 V2


@router.post("/api/verify_claims")
async def verify_claims_endpoint(request: Request, body: VerifyClaimsRequest):
    """
    Verify factual claims in synthesis against collected citations.

    POST /api/verify_claims
    {
        "answer": "synthesis text with claims",
        "citations": [{"url": "...", "title": "...", "content": "..."}],
        "use_v2": true  // optional, defaults to true
    }

    V1 Returns (use_v2=false):
    {
        "overall_confidence": 0.82,
        "total_claims": 10,
        "supported_claims": 8,
        "unsupported_claims": ["unsupported claim text"],
        "conflicts": [],
        "claim_details": [...]
    }

    V2 Returns (use_v2=true, default):
    {
        "overall_confidence": 0.72,
        "total_claims": 10,
        "supported_claims": 5,
        "unsupported_claims": 1,
        "insufficient_evidence_claims": 4,
        "supported_claim_texts": [...],
        "unsupported_claim_texts": [...],
        "insufficient_claim_texts": [...],
        "claim_details": [...],  // with verdict, retrieval_scores, reasoning
        "conflicts": [],
        "evidence_coverage": 0.6,
        "avg_retrieval_score": 3.2
    }
    """
    try:
        # Get LLM providers from app state
        # 从 app state 获取 LLM providers
        providers = request.app.state.providers

        if body.use_v2:
            # V2: BM25 retrieval + three-category output
            # V2：BM25 检索 + 三类判定输出
            result = await verify_claims_v2(
                answer=body.answer,
                citations=body.citations,
                llm_client=providers
            )
            return result.model_dump()
        else:
            # V1: Legacy verification
            # V1：旧版验证
            result = await verify_claims(
                answer=body.answer,
                citations=body.citations,
                llm_client=providers
            )
            return result.model_dump()

    except Exception as e:
        logger.error(f"[verify_claims_endpoint] Error: {e}", exc_info=True)
        # Return a safe default response on error
        # 出错时返回安全的默认响应
        if body.use_v2:
            return VerificationResultV2(
                overall_confidence=0.5,
                total_claims=0,
                supported_claims=0,
                unsupported_claims=0,
                insufficient_evidence_claims=0,
                evidence_coverage=0.0,
                avg_retrieval_score=0.0
            ).model_dump()
        else:
            return VerificationResult(
                overall_confidence=0.5,
                total_claims=0,
                supported_claims=0,
                unsupported_claims=[],
                conflicts=[]
            ).model_dump()


# ============================================================================
# Citation V2: Batch Verification Endpoint (2 LLM calls total)
# Citation V2：批量验证端点（共 2 次 LLM 调用）
# ============================================================================

class CitationWithIDInput(BaseModel):
    """Citation input with sequential ID from Go FilterFetchOnlyAndAssignIDs."""
    model_config = ConfigDict(extra="ignore")

    id: int  # Sequential ID (1, 2, 3...) 注：顺序 ID（1, 2, 3...）
    url: str = ""
    title: str = ""
    content: str | None = None
    snippet: str | None = None
    credibility_score: float = 0.5
    quality_score: float = 0.5  # P0-C: Added for ranking 注：P0-C：为排序而新增


class ClaimMapping(BaseModel):
    """Mapping of a claim to its supporting citations."""
    claim: str
    verdict: str = "insufficient_evidence"  # "supported" | "unsupported" | "insufficient_evidence" 注：枚举取值 supported / unsupported / insufficient_evidence
    supporting_ids: list[int] = Field(default_factory=list)  # Citation IDs that support this claim 注：支持该 claim 的 citation ID
    confidence: float = 0.0
    reasoning: str = ""


class VerifyBatchRequest(BaseModel):
    """Request body for batch verification endpoint."""
    answer: str  # Synthesis text containing claims 注：包含 claims 的 synthesis 文本
    citations: list[CitationWithIDInput]  # Citations with IDs from Go 注：来自 Go 的带 ID citations


class VerifyBatchResponse(BaseModel):
    """Response from batch verification."""
    claims: list[ClaimMapping] = Field(default_factory=list)
    total_claims: int = 0
    supported_count: int = 0
    unsupported_count: int = 0
    insufficient_count: int = 0


def _get_adaptive_topk(total_sources: int, total_claims: int) -> int:
    """
    P0-C: Dynamically adjust K value based on sources and claims count.

    Balances coverage vs cost:
    - More sources → larger K for better coverage
    - More claims → smaller K to control total evaluations
    - Total evaluations capped at 100 to prevent LLM overload

    Args:
        total_sources: Number of available citations
        total_claims: Number of claims to verify

    Returns:
        Optimal K value (3-10)
    """
    # Base K depends on source count
    # 基础 K 取决于 source 数量
    if total_sources >= 50:
        base_k = 10
    elif total_sources >= 20:
        base_k = 7
    else:
        base_k = 5

    # Cost control: total evaluations should not exceed 100
    # 成本控制：总评估次数不应超过 100
    max_evaluations = 100
    if total_claims > 0 and total_claims * base_k > max_evaluations:
        base_k = max(3, max_evaluations // total_claims)

    # Ensure K doesn't exceed available sources
    # 确保 K 不超过可用 sources 数量
    return min(base_k, total_sources) if total_sources > 0 else 3


def _batch_bm25_scores(
    claims: list[str],
    citations: list[CitationWithIDInput],
    top_k: int = 5
) -> dict[str, list[tuple[int, float]]]:
    """
    Pre-compute BM25 scores for all (claim, citation) pairs.

    P0-C: Enhanced ranking formula:
    score = 0.5 * BM25 + 0.3 * quality + 0.2 * credibility

    Args:
        claims: List of claim strings
        citations: Citations with IDs
        top_k: Number of top citations per claim (P0-C: now configurable)

    Returns:
        Dict mapping claim → [(citation_id, score), ...] sorted by score descending
    """
    if not claims or not citations:
        return {}

    # Build corpus stats from citations
    # 从 citations 构建 corpus stats
    stats = CorpusStats.from_citations(citations)
    citation_tokens_map: dict[int, list[str]] = {
        c.id: tokenize(f"{c.title or ''} {c.content or c.snippet or ''}")
        for c in citations
    }

    # Score each (claim, citation) pair
    # 为每个 (claim, citation) 对打分
    result: dict[str, list[tuple[int, float]]] = {}

    for claim in claims:
        claim_tokens = tokenize(claim)
        if not claim_tokens:
            result[claim] = []
            continue

        scores: list[tuple[int, float]] = []
        for c in citations:
            doc_tokens = citation_tokens_map.get(c.id, [])
            if not doc_tokens:
                continue

            # BM25 with IDF
            # 带 IDF 的 BM25
            relevance = bm25_score(claim_tokens, doc_tokens, corpus_stats=stats)

            # P0-C: Enhanced ranking formula
            # score = 0.5 * relevance(BM25) + 0.3 * quality + 0.2 * credibility
            # Normalize BM25 to 0-1 range (assume max ~10)
            # P0-C：增强排序公式
            # 将 BM25 归一化到 0-1 区间（假设最大值约 10）
            final_score = (
                0.5 * min(1.0, relevance / 10.0) +
                0.3 * (c.quality_score * c.credibility_score) +
                0.2 * c.credibility_score
            )

            if final_score > 0:
                scores.append((c.id, final_score))

        # Sort by score descending and take top_k
        # 按得分降序排序并取 top_k
        scores.sort(key=lambda x: x[1], reverse=True)
        # P0-C: Return only top_k (handles < K case automatically)
        # P0-C：只返回 top_k（自动处理少于 K 的情况）
        result[claim] = scores[:top_k]

    return result


async def _batch_verify_all_claims(
    claims: list[str],
    citations: list[CitationWithIDInput],
    bm25_scores: dict[str, list[tuple[int, float]]],
    providers: Any
) -> list[ClaimMapping]:
    """
    Batch verify all claims in a single LLM call.

    P0-C: Improved to show each claim with its own top-5 evidence (per-claim organization)
    instead of flattening all sources together.

    Args:
        claims: List of extracted claims
        citations: Citations with IDs
        bm25_scores: Pre-computed BM25 scores per claim
        providers: LLM provider

    Returns:
        List of ClaimMapping with verdicts and supporting IDs
    """
    if not claims or not citations:
        return [ClaimMapping(claim=c, verdict="insufficient_evidence") for c in claims]

    # Build citation lookup
    # 构建 citation 查找表
    cid_to_citation = {c.id: c for c in citations}

    # P0-C: Build per-claim evidence context (each claim sees only its top-5)
    # P0-C：为每个 claim 构建证据上下文（每个 claim 只看自己的 top-5）
    claim_contexts = []
    for i, claim in enumerate(claims):
        top5 = bm25_scores.get(claim, [])[:5]
        evidence_parts = []
        valid_ids = []

        for cid, score in top5:
            c = cid_to_citation.get(cid)
            if c:
                text = (c.content or c.snippet or "")[:400]
                evidence_parts.append(f"  [{cid}] (relevance:{score:.1f}) {c.title or c.url}\n  {text}")
                valid_ids.append(cid)

        claim_contexts.append({
            "index": i + 1,
            "claim": claim,
            "evidence": "\n\n".join(evidence_parts) if evidence_parts else "(no relevant evidence)",
            "valid_ids": valid_ids
        })

    # Language detection
    # 语言检测
    lang = detect_language(claims[0] if claims else "")

    # P0-C: Build structured prompt with per-claim evidence
    # P0-C：构建含每个 claim 证据的结构化 prompt
    if lang == "zh":
        prompt_parts = ["判断以下每个陈述是否被其对应的证据支持。\n\n每个陈述只需要看它自己的证据片段（已按相关性排序）。\n"]

        for ctx in claim_contexts:
            prompt_parts.append(f"""
---
## 陈述 {ctx['index']}: {ctx['claim']}

### 相关证据:
{ctx['evidence']}

### 可用 citation IDs: {ctx['valid_ids']}
---
""")

        prompt_parts.append("""
## 输出要求
只输出一个 JSON 数组，例如：
```json
[
  {"claim_index": 1, "verdict": "supported", "supporting_ids": [1], "reasoning": "简短解释"},
  {"claim_index": 2, "verdict": "insufficient_evidence", "supporting_ids": [], "reasoning": "简短解释"}
]
```

## 判定标准
- **supported**: 证据中明确包含支持该陈述的内容
- **unsupported**: 证据中明确反驳该陈述
- **insufficient_evidence**: 证据不足以判断

## 约束
- verdict 必须是 "supported"、"unsupported" 或 "insufficient_evidence"
- supporting_ids 只能包含该陈述对应的可用 citation IDs（见上面每段的 "可用 citation IDs"）

只输出 JSON 数组，无其他内容。
""")
        prompt = "".join(prompt_parts)

    else:
        prompt_parts = ["Judge whether each claim is supported by its corresponding evidence.\n\nEach claim should only be evaluated against its own evidence snippets (sorted by relevance).\n"]

        for ctx in claim_contexts:
            prompt_parts.append(f"""
---
## Claim {ctx['index']}: {ctx['claim']}

### Relevant Evidence:
{ctx['evidence']}

### Available citation IDs: {ctx['valid_ids']}
---
""")

        prompt_parts.append("""
## Output Format
Output only a JSON array, for example:
```json
[
  {"claim_index": 1, "verdict": "supported", "supporting_ids": [1], "reasoning": "brief explanation"},
  {"claim_index": 2, "verdict": "insufficient_evidence", "supporting_ids": [], "reasoning": "brief explanation"}
]
```

## Judgment Criteria
- **supported**: Evidence explicitly contains content supporting the claim
- **unsupported**: Evidence explicitly contradicts the claim
- **insufficient_evidence**: Evidence is inconclusive

## Constraints
- verdict must be one of: "supported", "unsupported", "insufficient_evidence"
- supporting_ids must only include citation IDs available for that claim (shown above)

Output only the JSON array, nothing else.
""")
        prompt = "".join(prompt_parts)

    try:
        from llm_service.providers.base import ModelTier

        result = await providers.generate_completion(
            messages=[{"role": "user", "content": prompt}],
            tier=ModelTier.SMALL,
            max_tokens=4000,  # Need space for all claim judgments 注：需为所有 claim 判定留出空间
            temperature=0.0,
            cache_source="verify_batch",
        )

        response = result.get("output_text", "").strip()

        # Parse JSON array
        # 解析 JSON 数组
        json_start = response.find('[')
        json_end = response.rfind(']') + 1
        if json_start != -1 and json_end > json_start:
            parsed = json.loads(response[json_start:json_end])
        else:
            parsed = json.loads(response)

        # Build result mappings
        # 构建结果映射
        mappings: list[ClaimMapping] = []
        parsed_by_index = {item.get("claim_index", i+1): item for i, item in enumerate(parsed)}

        valid_ids = {c.id for c in citations}

        for i, claim in enumerate(claims):
            item = parsed_by_index.get(i + 1, {})
            verdict = item.get("verdict", "insufficient_evidence")
            if verdict not in ("supported", "unsupported", "insufficient_evidence"):
                verdict = "insufficient_evidence"

            # Filter supporting_ids to valid citation IDs
            # 将 supporting_ids 过滤为有效 citation IDs
            supporting_ids = [sid for sid in item.get("supporting_ids", []) if sid in valid_ids]
            reasoning = item.get("reasoning", "")

            # Calculate confidence based on verdict and BM25 scores
            # 按 verdict 与 BM25 得分计算 confidence
            if verdict == "supported" and supporting_ids:
                # Use BM25 score of best supporting citation
                # 使用最佳 supporting citation 的 BM25 得分
                top_bm25 = bm25_scores.get(claim, [])
                bm25_map = {cid: score for cid, score in top_bm25}
                max_score = max((bm25_map.get(sid, 0) for sid in supporting_ids), default=0)
                confidence = min(0.95, 0.6 + 0.35 * min(1.0, max_score / 5.0))
            elif verdict == "unsupported":
                confidence = 0.8
            else:
                confidence = 0.3

            mappings.append(ClaimMapping(
                claim=claim,
                verdict=verdict,
                supporting_ids=supporting_ids,
                confidence=confidence,
                reasoning=reasoning
            ))

        return mappings

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"[verify_batch] Failed to parse batch response: {e}")
        return [ClaimMapping(claim=c, verdict="insufficient_evidence", reasoning=f"Parse error: {e}") for c in claims]
    except Exception as e:
        logger.error(f"[verify_batch] Unexpected error: {e}")
        return [ClaimMapping(claim=c, verdict="insufficient_evidence", reasoning=f"Error: {e}") for c in claims]


@router.post("/api/verify_batch")
async def verify_batch_endpoint(request: Request, body: VerifyBatchRequest):
    """
    Citation V2: Batch verification with 2 LLM calls.

    This endpoint is designed for Deep Research workflow:
    1. Takes fetch-only citations with sequential IDs (from Go FilterFetchOnlyAndAssignIDs)
    2. Extracts claims from answer (1 LLM call)
    3. Pre-computes BM25 scores for all (claim, citation) pairs (no LLM)
    4. Batch verifies all claims (1 LLM call)
    5. Returns ClaimMappings for Citation Agent V2

    POST /api/verify_batch
    {
        "answer": "synthesis text with claims",
        "citations": [
            {"id": 1, "url": "...", "title": "...", "content": "..."},
            {"id": 2, "url": "...", "title": "...", "snippet": "..."}
        ]
    }

    Returns:
    {
        "claims": [
            {"claim": "...", "verdict": "supported", "supporting_ids": [1, 3], "confidence": 0.85},
            {"claim": "...", "verdict": "insufficient_evidence", "supporting_ids": [], "confidence": 0.3}
        ],
        "total_claims": 10,
        "supported_count": 6,
        "unsupported_count": 1,
        "insufficient_count": 3
    }
    """
    try:
        providers = request.app.state.providers

        # Step 1: Extract claims (1 LLM call)
        # 步骤 1：提取 claims（1 次 LLM 调用）
        claims = await _extract_claims(body.answer, providers)
        logger.info(f"[verify_batch] Extracted {len(claims)} claims")

        if not claims:
            return VerifyBatchResponse(
                claims=[],
                total_claims=0,
                supported_count=0,
                unsupported_count=0,
                insufficient_count=0
            ).model_dump()

        # P0-C: Calculate adaptive top_k based on sources and claims count
        # P0-C：按 sources 与 claims 数量计算自适应 top_k
        top_k = _get_adaptive_topk(len(body.citations), len(claims))
        logger.info(f"[verify_batch] Using adaptive top_k={top_k} "
                    f"(sources={len(body.citations)}, claims={len(claims)})")

        # Step 2: Pre-compute BM25 scores (no LLM)
        # 步骤 2：预计算 BM25 得分（不调用 LLM）
        bm25_scores = _batch_bm25_scores(claims, body.citations, top_k=top_k)
        logger.debug(f"[verify_batch] Computed BM25 scores for {len(claims)} claims × {len(body.citations)} citations")

        # Step 3: Batch verify all claims (1 LLM call)
        # 步骤 3：批量验证所有 claims（1 次 LLM 调用）
        mappings = await _batch_verify_all_claims(claims, body.citations, bm25_scores, providers)

        # Count verdicts
        # 统计 verdict
        supported_count = sum(1 for m in mappings if m.verdict == "supported")
        unsupported_count = sum(1 for m in mappings if m.verdict == "unsupported")
        insufficient_count = sum(1 for m in mappings if m.verdict == "insufficient_evidence")

        logger.info(f"[verify_batch] Results: supported={supported_count}, "
                    f"unsupported={unsupported_count}, insufficient={insufficient_count}")

        return VerifyBatchResponse(
            claims=mappings,
            total_claims=len(claims),
            supported_count=supported_count,
            unsupported_count=unsupported_count,
            insufficient_count=insufficient_count
        ).model_dump()

    except Exception as e:
        logger.error(f"[verify_batch] Error: {e}", exc_info=True)
        return VerifyBatchResponse(
            claims=[],
            total_claims=0,
            supported_count=0,
            unsupported_count=0,
            insufficient_count=0
        ).model_dump()
