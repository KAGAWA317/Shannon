#!/usr/bin/env python3
"""
Shannon Platform - Qdrant Vector Database Collections Setup
GitHub: https://github.com/Kocoro-lab/Shannon
"""

import asyncio
import os
import sys
import time
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    CollectionInfo,
    OptimizersConfigDiff,
    HnswConfigDiff
)

# Configuration
# 配置
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
QDRANT_URL = os.getenv("QDRANT_URL", None)  # Support URL format from Docker 注：兼容 Docker 提供的 URL 形式
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", 1536))  # OpenAI ada-002 dimension 注：OpenAI ada-002 的向量维度

def wait_for_qdrant(client, max_retries=30, delay=2):
    """Wait for Qdrant to be ready."""
    for i in range(max_retries):
        try:
            # Try to get collections list to check connectivity
            # 获取 collection 列表以检查连通性
            client.get_collections()
            print("Qdrant is ready")
            return True
        except Exception as e:
            if i < max_retries - 1:
                print(f"Waiting for Qdrant... ({i+1}/{max_retries})")
                time.sleep(delay)
            else:
                print(f"Failed to connect to Qdrant after {max_retries} attempts", file=sys.stderr)
                raise
    return False

async def create_collections():
    """Create Qdrant collections for Shannon platform"""
    
    # Support both URL and host:port format
    # 同时支持 URL 与 host:port 两种格式
    if QDRANT_URL:
        client = QdrantClient(url=QDRANT_URL)
    else:
        client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    
    # Wait for Qdrant to be ready
    # 等待 Qdrant 就绪
    wait_for_qdrant(client)
    
    collections = [
        {
            "name": "task_embeddings",
            "description": "Embeddings of user tasks for similarity search",
            "vector_size": EMBEDDING_DIM,
            "distance": Distance.COSINE,
        },
        {
            "name": "tool_results",
            "description": "Cached tool execution results",
            "vector_size": EMBEDDING_DIM,
            "distance": Distance.COSINE,
        },
        {
            "name": "cases",  # Keep original name for compatibility 注：保留原名称以兼容旧数据
            "description": "Case-based learning memory",
            "vector_size": EMBEDDING_DIM,
            "distance": Distance.COSINE,
        },
        {
            "name": "document_chunks",
            "description": "Document chunks for RAG",
            "vector_size": EMBEDDING_DIM,
            "distance": Distance.COSINE,
        },
        {
            "name": "summaries",
            "description": "Compressed historical context summaries",
            "vector_size": EMBEDDING_DIM,
            "distance": Distance.COSINE,
        }
    ]
    
    for collection in collections:
        try:
            # Check if collection exists
            # 检查 collection 是否存在
            existing = client.get_collections()
            if any(c.name == collection["name"] for c in existing.collections):
                print(f"Collection {collection['name']} already exists")
                continue
            
            # Create collection with optimized settings
            # 用优化后的配置创建 collection
            client.create_collection(
                collection_name=collection["name"],
                vectors_config=VectorParams(
                    size=collection["vector_size"],
                    distance=collection["distance"]
                ),
                optimizers_config=OptimizersConfigDiff(
                    indexing_threshold=20000,  # Start indexing after 20k points 注：超过 2 万个点后开始建索引
                    memmap_threshold=50000,    # Use memmap after 50k points 注：超过 5 万个点后使用 memmap
                ),
                hnsw_config=HnswConfigDiff(
                    m=16,  # Number of edges per node 注：每个节点的边数
                    ef_construct=100,  # Size of dynamic candidate list 注：动态候选列表的大小
                    full_scan_threshold=10000,  # Use full scan for small collections 注：小规模 collection 走全量扫描
                )
            )
            
            print(f"Created collection: {collection['name']}")
            
            # Create indexes on payload fields
            # 为 payload 字段创建索引
            if collection["name"] == "task_embeddings":
                # Core filtering indexes
                # 核心过滤索引
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="session_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="tenant_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="user_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="agent_id",
                    field_schema="keyword"
                )
                # Chunking-specific indexes
                # 分块相关索引
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="qa_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="is_chunked",
                    field_schema="bool"
                )
                # Temporal index for recent retrieval
                # 用于近期检索的时间索引
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="timestamp",
                    field_schema="integer"
                )
                
            elif collection["name"] == "tool_results":
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="tool_name",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="timestamp",
                    field_schema="integer"
                )
                
            elif collection["name"] == "cases":
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="reward",
                    field_schema="float"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="user_id",
                    field_schema="keyword"
                )
                
            elif collection["name"] == "document_chunks":
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="document_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="chunk_index",
                    field_schema="integer"
                )

            elif collection["name"] == "summaries":
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="session_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="tenant_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="user_id",
                    field_schema="keyword"
                )
                client.create_payload_index(
                    collection_name=collection["name"],
                    field_name="timestamp",
                    field_schema="integer"
                )
            
            print(f"Created indexes for collection: {collection['name']}")
            
        except Exception as e:
            print(f"Error creating collection {collection['name']}: {e}")
    
    # Verify collections
    # 校验各 collection
    collections_info = client.get_collections()
    print(f"\nTotal collections: {len(collections_info.collections)}")
    for collection in collections_info.collections:
        info = client.get_collection(collection.name)
        print(f"  - {collection.name}: {info.points_count} points, {info.vectors_count} vectors")

async def seed_sample_data():
    """Seed sample data for testing"""
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    
    # Sample embeddings (random for demonstration)
    # 示例 embedding（随机生成，仅用于演示）
    import random
    sample_embedding = [random.random() for _ in range(EMBEDDING_DIM)]
    
    # Add sample points to task_embeddings
    # 向 task_embeddings 写入示例点位
    points = [
        PointStruct(
            id=1,
            vector=sample_embedding,
            payload={
                "task_id": "task-001",
                "user_id": "user-001",
                "session_id": "session-001",
                "query": "What is the weather today?",
                "timestamp": 1704067200
            }
        )
    ]
    
    client.upsert(
        collection_name="task_embeddings",
        points=points
    )
    
    print("Sample data seeded successfully")

if __name__ == "__main__":
    asyncio.run(create_collections())
    
    # Optionally seed sample data
    # 可选：写入示例数据
    if os.getenv("SEED_DATA", "false").lower() == "true":
        asyncio.run(seed_sample_data())