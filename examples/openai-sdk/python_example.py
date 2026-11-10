#!/usr/bin/env python3
"""
Shannon OpenAI-Compatible API - Python Example

This example demonstrates how to use Shannon's research capabilities
through the standard OpenAI Python SDK.

本示例演示如何通过标准 OpenAI Python SDK 使用 Shannon 的研究能力。

Requirements:
    pip install openai

Usage:
    export SHANNON_API_KEY="sk-shannon-your-api-key"
    python python_example.py
"""

import os
from openai import OpenAI

# Configuration
# 配置
API_KEY = os.getenv("SHANNON_API_KEY", "sk-shannon-your-api-key")
BASE_URL = os.getenv("SHANNON_BASE_URL", "https://api.shannon.run/v1")


def create_client():
    """Create an OpenAI client configured for Shannon.

    创建一个指向 Shannon 的 OpenAI client。
    """
    return OpenAI(
        api_key=API_KEY,
        base_url=BASE_URL
    )


def list_models():
    """List available Shannon models.

    列出可用的 Shannon 模型。
    """
    client = create_client()
    models = client.models.list()

    print("Available Models:")
    print("-" * 40)
    for model in models.data:
        print(f"  - {model.id}")
    print()


def simple_chat():
    """Simple non-streaming chat completion.

    简单的非流式 chat completion。
    """
    client = create_client()

    print("Simple Chat Example")
    print("-" * 40)

    response = client.chat.completions.create(
        model="shannon-chat",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What is the capital of France?"}
        ]
    )

    print(f"Response: {response.choices[0].message.content}")
    print(f"Tokens used: {response.usage.total_tokens}")
    print()


def streaming_chat():
    """Streaming chat completion.

    流式 chat completion。
    """
    client = create_client()

    print("Streaming Chat Example")
    print("-" * 40)

    stream = client.chat.completions.create(
        model="shannon-chat",
        messages=[
            {"role": "user", "content": "Write a haiku about programming."}
        ],
        stream=True
    )

    print("Response: ", end="")
    for chunk in stream:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
    print("\n")


def deep_research():
    """Deep research with iterative refinement.

    带迭代改进的深度研究。
    """
    client = create_client()

    print("Deep Research Example")
    print("-" * 40)
    print("Note: This may take 1-3 minutes for comprehensive research.\n")

    stream = client.chat.completions.create(
        model="shannon-deep-research",
        messages=[
            {"role": "system", "content": "You are a research analyst."},
            {"role": "user", "content": "Research the current state of AI regulation in the EU and US. Include recent developments from 2024."}
        ],
        stream=True,
        stream_options={"include_usage": True}
    )

    print("Research Output:")
    print("-" * 40)
    for chunk in stream:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)

        # Final chunk may include usage
        # 最后一个 chunk 可能带 usage
        if chunk.usage:
            print(f"\n\nTokens: prompt={chunk.usage.prompt_tokens}, completion={chunk.usage.completion_tokens}")
    print("\n")


def multi_turn_conversation():
    """Multi-turn conversation with session management.

    带 session 管理的多轮对话。
    """
    client = create_client()

    print("Multi-Turn Conversation Example")
    print("-" * 40)

    # First message
    # 第一条消息
    messages = [
        {"role": "system", "content": "You are a helpful assistant that remembers context."},
        {"role": "user", "content": "My name is Alice and I'm learning Python."}
    ]

    response1 = client.chat.completions.create(
        model="shannon-chat",
        messages=messages
    )
    print(f"User: My name is Alice and I'm learning Python.")
    print(f"Assistant: {response1.choices[0].message.content}\n")

    # Add assistant response to history
    # 把 assistant 回复加入历史
    messages.append({"role": "assistant", "content": response1.choices[0].message.content})

    # Second message (tests context retention)
    # 第二条消息（检验 context 保持）
    messages.append({"role": "user", "content": "What's my name and what am I learning?"})

    response2 = client.chat.completions.create(
        model="shannon-chat",
        messages=messages
    )
    print(f"User: What's my name and what am I learning?")
    print(f"Assistant: {response2.choices[0].message.content}\n")


def main():
    print("=" * 60)
    print("Shannon OpenAI-Compatible API - Python Examples")
    print("=" * 60)
    print()

    try:
        # List available models
        # 列出可用模型
        list_models()

        # Simple chat
        simple_chat()

        # Streaming chat
        streaming_chat()

        # Multi-turn conversation
        multi_turn_conversation()

        # Uncomment for longer examples:
        # 需要更长的示例时取消注释：
        # deep_research()

    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure to set SHANNON_API_KEY environment variable.")


if __name__ == "__main__":
    main()
