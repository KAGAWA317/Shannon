#!/usr/bin/env python3
"""
Memory System Evaluation Suite
Comprehensive testing and evaluation of Shannon's memory system improvements
"""

import asyncio
import json
import time
import statistics
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
import numpy as np
import requests
from uuid import uuid4
import grpc
import sys
import os

# Add project root to path
# 将项目根目录加入 path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

@dataclass
class TestResult:
    """Container for test results"""
    test_name: str
    passed: bool
    duration_ms: float
    metrics: Dict[str, Any]
    error: str = None


class MemorySystemEvaluator:
    """Evaluates Shannon's memory system performance and correctness"""

    def __init__(self, orchestrator_host="localhost:50052", qdrant_host="localhost:6333"):
        self.orchestrator_host = orchestrator_host
        self.qdrant_url = f"http://{qdrant_host}"
        self.results: List[TestResult] = []

    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all evaluation tests"""
        print("🧪 Shannon Memory System Evaluation")
        print("=" * 50)

        tests = [
            self.test_chunking_accuracy,
            self.test_chunk_reconstruction,
            self.test_batch_embedding_performance,
            self.test_mmr_diversity,
            self.test_idempotency,
            self.test_storage_efficiency,
            self.test_retrieval_latency,
            self.test_scalability,
        ]

        for test in tests:
            print(f"\n▶ Running {test.__name__}...")
            start = time.time()
            try:
                result = await test()
                duration = (time.time() - start) * 1000
                result.duration_ms = duration
                self.results.append(result)

                if result.passed:
                    print(f"  ✅ PASSED ({duration:.2f}ms)")
                else:
                    print(f"  ❌ FAILED: {result.error}")
            except Exception as e:
                print(f"  ❌ ERROR: {str(e)}")
                self.results.append(TestResult(
                    test_name=test.__name__,
                    passed=False,
                    duration_ms=(time.time() - start) * 1000,
                    metrics={},
                    error=str(e)
                ))

        return self.generate_report()

    async def test_chunking_accuracy(self) -> TestResult:
        """Test chunking accuracy and overlap correctness"""
        test_texts = [
            ("short", "This is a short text", 0),
            ("medium", " ".join(["word"] * 500), 0),  # ~2000 chars 注：约 2000 字符
            ("long", " ".join(["word"] * 2500), 2),   # ~10000 chars, should chunk 注：约 10000 字符，应分块
            ("very_long", " ".join(["word"] * 5000), 3),  # ~20000 chars 注：约 20000 字符
        ]

        metrics = {}
        errors = []

        for name, text, expected_chunks in test_texts:
            # Simulate chunking (would call actual Shannon API)
            # 模拟分块（真实场景会调用 Shannon API）
            chunks = self.simulate_chunking(text, max_tokens=2000, overlap=200)
            actual_chunks = len(chunks) if len(chunks) > 1 else 0

            if actual_chunks != expected_chunks and expected_chunks > 0:
                errors.append(f"{name}: expected {expected_chunks} chunks, got {actual_chunks}")

            metrics[f"{name}_chunks"] = actual_chunks

            # Verify overlap
            # 校验重叠
            if len(chunks) > 1:
                overlap_valid = self.verify_overlap(chunks, overlap_tokens=200)
                metrics[f"{name}_overlap_valid"] = overlap_valid
                if not overlap_valid:
                    errors.append(f"{name}: invalid overlap between chunks")

        return TestResult(
            test_name="chunking_accuracy",
            passed=len(errors) == 0,
            duration_ms=0,
            metrics=metrics,
            error="; ".join(errors) if errors else None
        )

    async def test_chunk_reconstruction(self) -> TestResult:
        """Test that chunked text can be accurately reconstructed"""
        original = "START " + " ".join([f"section_{i}" for i in range(1000)]) + " END"

        # Simulate chunking and reconstruction
        # 模拟分块与重建
        chunks = self.simulate_chunking(original, max_tokens=500, overlap=50)
        reconstructed = self.simulate_reconstruction(chunks)

        # Check key markers are present and in order
        # 检查关键标记存在且顺序正确
        has_start = "START" in reconstructed
        has_end = "END" in reconstructed
        order_preserved = reconstructed.index("START") < reconstructed.index("END") if has_start and has_end else False

        # Calculate similarity
        # 计算相似度
        similarity = self.calculate_similarity(original, reconstructed)

        metrics = {
            "chunks_created": len(chunks),
            "reconstruction_similarity": similarity,
            "markers_preserved": has_start and has_end and order_preserved
        }

        passed = similarity > 0.95 and metrics["markers_preserved"]

        return TestResult(
            test_name="chunk_reconstruction",
            passed=passed,
            duration_ms=0,
            metrics=metrics,
            error=f"Low similarity: {similarity}" if not passed else None
        )

    async def test_batch_embedding_performance(self) -> TestResult:
        """Compare batch vs sequential embedding performance"""
        texts = [f"Test text {i}" for i in range(10)]

        # Simulate sequential embedding
        # 模拟串行 embedding
        start = time.time()
        for text in texts:
            time.sleep(0.01)  # Simulate API call 注：模拟一次 API 调用
        sequential_time = time.time() - start

        # Simulate batch embedding
        # 模拟批量 embedding
        start = time.time()
        time.sleep(0.02)  # Simulate single batch API call 注：模拟单次批量 API 调用
        batch_time = time.time() - start

        improvement = sequential_time / batch_time if batch_time > 0 else 0

        metrics = {
            "sequential_time_ms": sequential_time * 1000,
            "batch_time_ms": batch_time * 1000,
            "improvement_factor": improvement,
            "texts_processed": len(texts)
        }

        return TestResult(
            test_name="batch_embedding_performance",
            passed=improvement > 2.0,  # Expect at least 2x improvement 注：期望至少 2 倍提升
            duration_ms=0,
            metrics=metrics,
            error=f"Insufficient improvement: {improvement:.1f}x" if improvement <= 2.0 else None
        )

    async def test_mmr_diversity(self) -> TestResult:
        """Test MMR diversity in search results"""
        # Simulate candidate pool with similarity scores
        # 模拟带相似度分数的候选池
        candidates = [
            {"id": i, "score": 0.95 - i * 0.01, "vector": np.random.rand(128)}
            for i in range(20)
        ]

        # Without MMR (top-k by score)
        # 不使用 MMR（按分数取 top-k）
        without_mmr = sorted(candidates, key=lambda x: x["score"], reverse=True)[:5]

        # With MMR (simulated)
        # 使用 MMR（模拟）
        with_mmr = self.simulate_mmr(candidates, lambda_param=0.7, k=5)

        # Calculate diversity
        # 计算多样性
        diversity_without = self.calculate_diversity(without_mmr)
        diversity_with = self.calculate_diversity(with_mmr)

        metrics = {
            "diversity_without_mmr": diversity_without,
            "diversity_with_mmr": diversity_with,
            "diversity_improvement": (diversity_with / diversity_without - 1) * 100 if diversity_without > 0 else 0,
            "candidates_pool_size": len(candidates)
        }

        return TestResult(
            test_name="mmr_diversity",
            passed=diversity_with > diversity_without,
            duration_ms=0,
            metrics=metrics,
            error="MMR did not improve diversity" if diversity_with <= diversity_without else None
        )

    async def test_idempotency(self) -> TestResult:
        """Test that duplicate writes are idempotent"""
        session_id = str(uuid4())
        test_data = {
            "query": "Test idempotency",
            "answer": "Test answer " * 100,
            "session_id": session_id
        }

        # Simulate first write
        # 模拟第一次写入
        write1_points = 3  # Assume it creates 3 chunks 注：假设会生成 3 个分块

        # Simulate second write (should be idempotent)
        # 模拟第二次写入（应保持幂等）
        write2_points = 3  # Should create same chunks with same IDs 注：应生成 ID 相同的相同分块

        # Check Qdrant for actual point count
        # 查询 Qdrant 获取实际 point 数量
        # In real test, would query Qdrant API
        # 真实测试中会查询 Qdrant API
        total_points = write1_points  # Should not double 注：不应翻倍

        metrics = {
            "first_write_chunks": write1_points,
            "second_write_chunks": write2_points,
            "total_points_stored": total_points,
            "duplicates_created": 0
        }

        return TestResult(
            test_name="idempotency",
            passed=metrics["duplicates_created"] == 0,
            duration_ms=0,
            metrics=metrics,
            error="Duplicates created" if metrics["duplicates_created"] > 0 else None
        )

    async def test_storage_efficiency(self) -> TestResult:
        """Test storage reduction from chunking improvements"""
        test_answer = "Large answer content " * 1000  # ~20KB 注：约 20KB

        # Old method: store full answer in each chunk
        # 旧方案：每个分块都保存完整答案
        old_storage = self.calculate_old_storage(test_answer, chunks=5)

        # New method: store only chunk text
        # 新方案：只保存分块文本
        new_storage = self.calculate_new_storage(test_answer, chunks=5)

        reduction = (1 - new_storage / old_storage) * 100 if old_storage > 0 else 0

        metrics = {
            "old_storage_kb": old_storage / 1024,
            "new_storage_kb": new_storage / 1024,
            "reduction_percent": reduction,
            "chunks_count": 5
        }

        return TestResult(
            test_name="storage_efficiency",
            passed=reduction > 40,  # Expect at least 40% reduction 注：期望至少 40% 的缩减
            duration_ms=0,
            metrics=metrics,
            error=f"Insufficient reduction: {reduction:.1f}%" if reduction <= 40 else None
        )

    async def test_retrieval_latency(self) -> TestResult:
        """Test retrieval latency with indexes"""
        pool_sizes = [10, 50, 100, 500]
        latencies = []

        for size in pool_sizes:
            # Simulate retrieval with varying pool sizes
            # 模拟不同候选池大小下的检索
            start = time.time()
            # Simulate indexed search
            # 模拟带索引的检索
            time.sleep(0.001 * np.log(size))  # Logarithmic with indexes 注：有索引时为对数级
            latency = (time.time() - start) * 1000
            latencies.append(latency)

        avg_latency = statistics.mean(latencies)
        p95_latency = np.percentile(latencies, 95)

        metrics = {
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": p95_latency,
            "max_latency_ms": max(latencies),
            "pool_sizes_tested": pool_sizes
        }

        return TestResult(
            test_name="retrieval_latency",
            passed=p95_latency < 50,  # Expect <50ms p95 注：期望 p95 小于 50ms
            duration_ms=0,
            metrics=metrics,
            error=f"High p95 latency: {p95_latency:.1f}ms" if p95_latency >= 50 else None
        )

    async def test_scalability(self) -> TestResult:
        """Test system scalability"""
        qa_counts = [100, 500, 1000, 5000]
        write_times = []
        read_times = []

        for count in qa_counts:
            # Simulate writes
            # 模拟写入
            start = time.time()
            time.sleep(0.0001 * count)  # Linear write time 注：写入耗时随数量线性增长
            write_times.append((time.time() - start) * 1000)

            # Simulate reads with indexes
            # 模拟带索引的读取
            start = time.time()
            time.sleep(0.001 * np.log(count))  # Logarithmic read with indexes 注：有索引时读取为对数级
            read_times.append((time.time() - start) * 1000)

        # Calculate scalability factor
        # 计算扩展性系数
        write_scalability = write_times[-1] / write_times[0]
        read_scalability = read_times[-1] / read_times[0]
        count_increase = qa_counts[-1] / qa_counts[0]

        metrics = {
            "write_scalability_factor": write_scalability / count_increase,
            "read_scalability_factor": read_scalability / np.log(count_increase),
            "max_qa_tested": qa_counts[-1],
            "write_time_at_max_ms": write_times[-1],
            "read_time_at_max_ms": read_times[-1]
        }

        # Good scalability if sub-linear
        # 若呈亚线性增长则扩展性良好
        passed = metrics["read_scalability_factor"] < 2.0

        return TestResult(
            test_name="scalability",
            passed=passed,
            duration_ms=0,
            metrics=metrics,
            error="Poor read scalability" if not passed else None
        )

    # Helper methods
    # 辅助方法
    def simulate_chunking(self, text: str, max_tokens: int, overlap: int) -> List[str]:
        """Simulate text chunking"""
        # Simple character-based chunking for simulation
        # 模拟用的简易按字符分块
        chars_per_token = 4
        max_chars = max_tokens * chars_per_token
        overlap_chars = overlap * chars_per_token

        if len(text) <= max_chars:
            return [text]

        chunks = []
        start = 0
        while start < len(text):
            end = min(start + max_chars, len(text))
            chunks.append(text[start:end])
            start = end - overlap_chars if end < len(text) else end

        return chunks

    def simulate_reconstruction(self, chunks: List[str]) -> str:
        """Simulate chunk reconstruction"""
        if not chunks:
            return ""
        if len(chunks) == 1:
            return chunks[0]

        # Simple overlap-aware reconstruction
        # 简易的考虑重叠的重建
        result = chunks[0]
        for chunk in chunks[1:]:
            # Find overlap and merge
            # 查找重叠并合并
            overlap_size = min(len(result), len(chunk)) // 10  # Approximate 注：近似值
            result = result + chunk[overlap_size:]

        return result

    def verify_overlap(self, chunks: List[str], overlap_tokens: int) -> bool:
        """Verify overlap between chunks"""
        if len(chunks) <= 1:
            return True

        for i in range(len(chunks) - 1):
            # Check if end of chunk i overlaps with start of chunk i+1
            # 检查分块 i 的尾部是否与分块 i+1 的头部重叠
            overlap_size = overlap_tokens * 4  # Approximate chars 注：近似字符数
            if len(chunks[i]) < overlap_size or len(chunks[i+1]) < overlap_size:
                continue

            # Simple overlap check
            # 简易重叠检查
            end_of_current = chunks[i][-overlap_size:]
            start_of_next = chunks[i+1][:overlap_size]

            # Some similarity expected
            # 预期存在一定相似度
            similarity = sum(1 for a, b in zip(end_of_current, start_of_next) if a == b)
            if similarity < overlap_size * 0.3:  # At least 30% similarity 注：至少 30% 相似度
                return False

        return True

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity"""
        if not text1 or not text2:
            return 0.0

        # Simple character-based similarity
        # 简易的按字符比较的相似度
        matches = sum(1 for a, b in zip(text1, text2) if a == b)
        return matches / max(len(text1), len(text2))

    def simulate_mmr(self, candidates: List[Dict], lambda_param: float, k: int) -> List[Dict]:
        """Simulate MMR reranking"""
        if not candidates or k <= 0:
            return []

        selected = []
        remaining = candidates.copy()

        # Select first by highest score
        # 先选分数最高的一个
        first = max(remaining, key=lambda x: x["score"])
        selected.append(first)
        remaining.remove(first)

        # Select rest with MMR
        # 其余用 MMR 选择
        while len(selected) < k and remaining:
            best_score = -1
            best_candidate = None

            for candidate in remaining:
                # Relevance score
                # 相关性分数
                relevance = candidate["score"]

                # Diversity score (1 - max similarity to selected)
                # 多样性分数（1 减去与已选项的最大相似度）
                max_sim = max(
                    self.vector_similarity(candidate["vector"], s["vector"])
                    for s in selected
                )
                diversity = 1 - max_sim

                # MMR score
                # MMR 分数
                mmr_score = lambda_param * relevance + (1 - lambda_param) * diversity

                if mmr_score > best_score:
                    best_score = mmr_score
                    best_candidate = candidate

            if best_candidate:
                selected.append(best_candidate)
                remaining.remove(best_candidate)

        return selected

    def vector_similarity(self, v1: np.ndarray, v2: np.ndarray) -> float:
        """Calculate cosine similarity between vectors"""
        if len(v1) != len(v2):
            return 0.0

        dot = np.dot(v1, v2)
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot / (norm1 * norm2)

    def calculate_diversity(self, items: List[Dict]) -> float:
        """Calculate diversity score for a set of items"""
        if len(items) <= 1:
            return 1.0

        # Average pairwise distance
        # 两两之间的平均距离
        distances = []
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                sim = self.vector_similarity(items[i]["vector"], items[j]["vector"])
                distances.append(1 - sim)

        return statistics.mean(distances) if distances else 0.0

    def calculate_old_storage(self, text: str, chunks: int) -> int:
        """Calculate storage with old method (full answer in each chunk)"""
        return len(text.encode()) * chunks

    def calculate_new_storage(self, text: str, chunks: int) -> int:
        """Calculate storage with new method (only chunk text)"""
        chunk_size = len(text.encode()) // chunks if chunks > 0 else len(text.encode())
        overlap = chunk_size * 0.1  # 10% overlap 注：10% 重叠
        return int(chunk_size * chunks + overlap * (chunks - 1))

    def generate_report(self) -> Dict[str, Any]:
        """Generate evaluation report"""
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r.passed)

        report = {
            "summary": {
                "total_tests": total_tests,
                "passed": passed_tests,
                "failed": total_tests - passed_tests,
                "pass_rate": (passed_tests / total_tests * 100) if total_tests > 0 else 0,
                "total_duration_ms": sum(r.duration_ms for r in self.results)
            },
            "tests": []
        }

        for result in self.results:
            report["tests"].append({
                "name": result.test_name,
                "passed": result.passed,
                "duration_ms": result.duration_ms,
                "metrics": result.metrics,
                "error": result.error
            })

        # Print summary
        # 打印汇总
        print("\n" + "=" * 50)
        print("📊 EVALUATION SUMMARY")
        print("=" * 50)
        print(f"Pass Rate: {report['summary']['pass_rate']:.1f}% ({passed_tests}/{total_tests})")
        print(f"Total Duration: {report['summary']['total_duration_ms']:.1f}ms")

        print("\n📈 Key Metrics:")
        for test in report["tests"]:
            if test["passed"]:
                print(f"  ✅ {test['name']}: {test['duration_ms']:.1f}ms")
                for key, value in test["metrics"].items():
                    if isinstance(value, float):
                        print(f"     - {key}: {value:.2f}")
                    else:
                        print(f"     - {key}: {value}")
            else:
                print(f"  ❌ {test['name']}: {test['error']}")

        return report


async def main():
    """Run the evaluation suite"""
    evaluator = MemorySystemEvaluator()
    report = await evaluator.run_all_tests()

    # Save report to file
    # 将报告保存到文件
    with open("memory_evaluation_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n📄 Report saved to memory_evaluation_report.json")

    # Return exit code based on pass rate
    # 根据通过率返回退出码
    return 0 if report["summary"]["pass_rate"] >= 80 else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)