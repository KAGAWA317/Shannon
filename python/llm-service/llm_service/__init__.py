"""Shannon LLM Service - Provider-agnostic LLM integration

Shannon LLM Service —— 与 provider 无关的 LLM 集成。
"""

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Add grpc_gen to sys.path for protobuf imports
# 将 grpc_gen 加入 sys.path，供 protobuf 导入使用
_grpc_gen = Path(__file__).parent / "grpc_gen"
if _grpc_gen.exists():
    sys.path.insert(0, str(_grpc_gen))
    logger.debug(f"Added grpc_gen to sys.path: {_grpc_gen}")
else:
    logger.warning(
        f"grpc_gen directory not found at {_grpc_gen}, protobuf imports may fail"
    )

__version__ = "0.1.0"
