"""Public deep-module interfaces for Remote Robot Protocol v2."""

from .dashboard import DashboardServer
from .operations import OperationsProjection
from .orchestrator import RunOrchestrator, RunResult
from .program import CompiledProgram, ProgramCompiler
from .registry import CapabilityRegistry


__all__ = [
    "CapabilityRegistry",
    "CompiledProgram",
    "DashboardServer",
    "OperationsProjection",
    "ProgramCompiler",
    "RunOrchestrator",
    "RunResult",
]
