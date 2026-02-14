"""
AI Agents Package
LLM-powered code conversion, validation, and optimization

This package provides AI agent capabilities following Databricks GenAI patterns:

- ChatDatabricks for LangChain integration
- ReAct agents with tools (create_tool_calling_agent, AgentExecutor)
- MLflow autologging for experiment tracking
- Unity Catalog model registration

Usage:
    # Direct SDK mode (default)
    config = AgentConfig(provider='databricks', model='databricks-meta-llama-3-3-70b-instruct')
    agent = CodeConverterAgent(config)
    result = agent.execute({'code': glue_code})
    
    # LangChain mode with ReAct agent
    config = AgentConfig(provider='databricks', use_langchain=True)
    agent = CodeConverterAgent(config)
    tools = get_code_converter_tools()
    react_agent = agent.create_react_agent(tools)
    result = react_agent.invoke({"input": "Convert this code..."})
    
    # Hybrid (rule-based + LLM)
    from glue2lakehouse.agents.code_converter_agent import HybridConverter
    converter = HybridConverter(rule_migrator, llm_agent)
    result = converter.convert(code, strategy='hybrid')

Supported Providers:
    - databricks: Databricks Foundation Model APIs (Llama-3-3, DBRX)
    - openai: OpenAI GPT-4
    - anthropic: Claude 3.5 Sonnet
    - azure: Azure OpenAI
"""

from glue2lakehouse.agents.base_agent import BaseAgent, AgentConfig, AgentResponse
from glue2lakehouse.agents.code_converter_agent import (
    CodeConverterAgent,
    HybridConverter,
    get_code_converter_tools
)
from glue2lakehouse.agents.validation_agent import ValidationAgent
from glue2lakehouse.agents.optimization_agent import OptimizationAgent
from glue2lakehouse.agents.agent_orchestrator import AgentOrchestrator

__all__ = [
    'BaseAgent',
    'AgentConfig',
    'AgentResponse',
    'CodeConverterAgent',
    'HybridConverter',
    'get_code_converter_tools',
    'ValidationAgent',
    'OptimizationAgent',
    'AgentOrchestrator'
]
