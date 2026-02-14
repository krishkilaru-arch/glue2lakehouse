"""
Base Agent Class
Foundation for all AI agents in the migration framework

Follows Databricks GenAI patterns:
- ChatDatabricks for LangChain integration
- MLflow autologging for experiment tracking
- AgentExecutor for ReAct-style reasoning
- Unity Catalog model registration

Author: Analytics360
Version: 2.1.0
"""

from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import logging
import time
import json

logger = logging.getLogger(__name__)

# Enable MLflow autologging for LangChain if available
try:
    import mlflow
    mlflow.langchain.autolog()
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    logger.info("MLflow not available - experiment tracking disabled")


@dataclass
class AgentConfig:
    """
    Configuration for AI agents.
    
    Supports both direct SDK calls and LangChain integration.
    
    Example:
        ```python
        # Direct SDK (default)
        config = AgentConfig(provider='databricks', model='databricks-meta-llama-3-3-70b-instruct')
        
        # LangChain mode (recommended for agents with tools)
        config = AgentConfig(
            provider='databricks',
            model='databricks-meta-llama-3-3-70b-instruct',
            use_langchain=True
        )
        ```
    """
    provider: str = 'databricks'  # 'databricks', 'openai', 'anthropic', 'azure'
    model: str = 'databricks-meta-llama-3-3-70b-instruct'  # Updated to Llama-3-3 per Databricks course
    temperature: float = 0.2
    max_tokens: int = 4000
    timeout: int = 60
    retry_attempts: int = 3
    api_key: Optional[str] = None
    endpoint: Optional[str] = None
    cost_tracking: bool = True
    audit_logging: bool = True
    # LangChain integration (Databricks recommended pattern)
    use_langchain: bool = False  # Use ChatDatabricks for LangChain integration
    use_react_agent: bool = False  # Use ReAct-style agent with tools
    mlflow_experiment: Optional[str] = None  # MLflow experiment name
    unity_catalog_model: Optional[str] = None  # UC model path for registration


@dataclass
class AgentResponse:
    """Response from an AI agent."""
    success: bool
    output: str
    reasoning: str
    confidence: float  # 0.0 to 1.0
    token_usage: Dict[str, int]
    cost_usd: float
    duration_seconds: float
    errors: List[str]
    warnings: List[str]
    metadata: Dict[str, Any]


class BaseAgent(ABC):
    """
    Base class for all AI agents.
    
    Provides common functionality:
    - LLM API integration
    - Retry logic
    - Cost tracking
    - Audit logging
    - Error handling
    
    Example:
        ```python
        class MyAgent(BaseAgent):
            def process(self, input_data):
                prompt = self._build_prompt(input_data)
                return self._call_llm(prompt)
        
        agent = MyAgent(config)
        result = agent.execute(data)
        ```
    """
    
    # Pricing per 1M tokens (approximate, update with actual rates)
    PRICING = {
        'databricks-dbrx-instruct': {'input': 0.75, 'output': 2.25},
        'databricks-meta-llama-3-70b-instruct': {'input': 0.5, 'output': 1.5},
        'gpt-4': {'input': 30.0, 'output': 60.0},
        'gpt-4-turbo': {'input': 10.0, 'output': 30.0},
        'claude-3-5-sonnet': {'input': 3.0, 'output': 15.0},
        'claude-3-opus': {'input': 15.0, 'output': 75.0}
    }
    
    def __init__(self, config: AgentConfig):
        """
        Initialize agent.
        
        Args:
            config: Agent configuration
        """
        self.config = config
        self.total_cost = 0.0
        self.total_calls = 0
        self.client = self._initialize_client()
        
        logger.info(f"{self.__class__.__name__} initialized with {config.provider}/{config.model}")
    
    def _initialize_client(self):
        """Initialize LLM client based on provider and mode."""
        # Use LangChain ChatDatabricks if configured (Databricks recommended pattern)
        if self.config.use_langchain and self.config.provider == 'databricks':
            return self._init_langchain_databricks_client()
        
        # Direct SDK clients
        if self.config.provider == 'databricks':
            return self._init_databricks_client()
        elif self.config.provider == 'openai':
            return self._init_openai_client()
        elif self.config.provider == 'anthropic':
            return self._init_anthropic_client()
        elif self.config.provider == 'azure':
            return self._init_azure_client()
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}")
    
    def _init_langchain_databricks_client(self):
        """
        Initialize ChatDatabricks client from langchain_databricks.
        
        This is the recommended pattern from Databricks GenAI course.
        Enables AgentExecutor, tools, and MLflow autologging.
        """
        try:
            from langchain_databricks import ChatDatabricks
            
            llm = ChatDatabricks(
                endpoint=self.config.model,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature
            )
            
            logger.info(f"Initialized ChatDatabricks with endpoint: {self.config.model}")
            return llm
            
        except ImportError:
            logger.warning("langchain_databricks not installed. Install: pip install langchain-databricks")
            # Fall back to SDK client
            return self._init_databricks_client()
    
    def _init_databricks_client(self):
        """Initialize Databricks Foundation Model API client (SDK)."""
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
            
            return WorkspaceClient()
        except ImportError:
            logger.warning("Databricks SDK not installed. Install: pip install databricks-sdk")
            return None
    
    def _init_openai_client(self):
        """Initialize OpenAI client."""
        try:
            from openai import OpenAI
            return OpenAI(api_key=self.config.api_key)
        except ImportError:
            logger.warning("OpenAI SDK not installed. Install: pip install openai")
            return None
    
    def _init_anthropic_client(self):
        """Initialize Anthropic client."""
        try:
            from anthropic import Anthropic
            return Anthropic(api_key=self.config.api_key)
        except ImportError:
            logger.warning("Anthropic SDK not installed. Install: pip install anthropic")
            return None
    
    def _init_azure_client(self):
        """Initialize Azure OpenAI client."""
        try:
            from openai import AzureOpenAI
            return AzureOpenAI(
                api_key=self.config.api_key,
                azure_endpoint=self.config.endpoint,
                api_version="2024-02-15-preview"
            )
        except ImportError:
            logger.warning("Azure OpenAI SDK not installed. Install: pip install openai")
            return None
    
    @abstractmethod
    def _build_prompt(self, input_data: Any) -> str:
        """
        Build prompt for the LLM.
        
        Must be implemented by subclasses.
        
        Args:
            input_data: Input data to process
        
        Returns:
            Formatted prompt string
        """
        pass
    
    @abstractmethod
    def _parse_response(self, response: str) -> Any:
        """
        Parse LLM response.
        
        Must be implemented by subclasses.
        
        Args:
            response: Raw LLM response
        
        Returns:
            Parsed output
        """
        pass
    
    def execute(self, input_data: Any) -> AgentResponse:
        """
        Execute agent with input data.
        
        Args:
            input_data: Data to process
        
        Returns:
            AgentResponse with results
        """
        start_time = time.time()
        errors = []
        warnings = []
        
        try:
            # Build prompt
            prompt = self._build_prompt(input_data)
            
            # Call LLM with retry logic
            raw_response, token_usage = self._call_llm_with_retry(prompt)
            
            # Parse response
            parsed_output = self._parse_response(raw_response)
            
            # Calculate cost
            cost = self._calculate_cost(token_usage)
            self.total_cost += cost
            self.total_calls += 1
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Log if audit enabled
            if self.config.audit_logging:
                self._log_execution(prompt, raw_response, token_usage, cost)
            
            return AgentResponse(
                success=True,
                output=parsed_output,
                reasoning=raw_response,
                confidence=self._estimate_confidence(raw_response),
                token_usage=token_usage,
                cost_usd=cost,
                duration_seconds=duration,
                errors=errors,
                warnings=warnings,
                metadata={
                    'provider': self.config.provider,
                    'model': self.config.model,
                    'total_cost': self.total_cost,
                    'total_calls': self.total_calls
                }
            )
        
        except Exception as e:
            logger.error(f"Agent execution failed: {e}")
            return AgentResponse(
                success=False,
                output='',
                reasoning='',
                confidence=0.0,
                token_usage={},
                cost_usd=0.0,
                duration_seconds=time.time() - start_time,
                errors=[str(e)],
                warnings=warnings,
                metadata={}
            )
    
    def _call_llm_with_retry(self, prompt: str) -> tuple:
        """Call LLM with retry logic."""
        for attempt in range(self.config.retry_attempts):
            try:
                return self._call_llm(prompt)
            except Exception as e:
                logger.warning(f"LLM call attempt {attempt + 1} failed: {e}")
                if attempt == self.config.retry_attempts - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def _call_llm(self, prompt: str) -> tuple:
        """
        Call LLM API.
        
        Returns:
            (response_text, token_usage)
        """
        if self.config.provider == 'databricks':
            return self._call_databricks(prompt)
        elif self.config.provider == 'openai':
            return self._call_openai(prompt)
        elif self.config.provider == 'anthropic':
            return self._call_anthropic(prompt)
        elif self.config.provider == 'azure':
            return self._call_azure(prompt)
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}")
    
    def _call_databricks(self, prompt: str) -> tuple:
        """Call Databricks Foundation Model API."""
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        
        response = self.client.serving_endpoints.query(
            name=self.config.model,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content="You are an expert in AWS Glue and Databricks migrations."),
                ChatMessage(role=ChatMessageRole.USER, content=prompt)
            ],
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
        
        text = response.choices[0].message.content
        usage = {
            'input_tokens': response.usage.prompt_tokens,
            'output_tokens': response.usage.completion_tokens,
            'total_tokens': response.usage.total_tokens
        }
        
        return text, usage
    
    def _call_openai(self, prompt: str) -> tuple:
        """Call OpenAI API."""
        response = self.client.chat.completions.create(
            model=self.config.model,
            messages=[
                {"role": "system", "content": "You are an expert in AWS Glue and Databricks migrations."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
        
        text = response.choices[0].message.content
        usage = {
            'input_tokens': response.usage.prompt_tokens,
            'output_tokens': response.usage.completion_tokens,
            'total_tokens': response.usage.total_tokens
        }
        
        return text, usage
    
    def _call_anthropic(self, prompt: str) -> tuple:
        """Call Anthropic API."""
        response = self.client.messages.create(
            model=self.config.model,
            system="You are an expert in AWS Glue and Databricks migrations.",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
        
        text = response.content[0].text
        usage = {
            'input_tokens': response.usage.input_tokens,
            'output_tokens': response.usage.output_tokens,
            'total_tokens': response.usage.input_tokens + response.usage.output_tokens
        }
        
        return text, usage
    
    def _call_azure(self, prompt: str) -> tuple:
        """Call Azure OpenAI API."""
        response = self.client.chat.completions.create(
            model=self.config.model,
            messages=[
                {"role": "system", "content": "You are an expert in AWS Glue and Databricks migrations."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
        
        text = response.choices[0].message.content
        usage = {
            'input_tokens': response.usage.prompt_tokens,
            'output_tokens': response.usage.completion_tokens,
            'total_tokens': response.usage.total_tokens
        }
        
        return text, usage
    
    def _calculate_cost(self, token_usage: Dict[str, int]) -> float:
        """Calculate cost based on token usage."""
        if not self.config.cost_tracking:
            return 0.0
        
        pricing = self.PRICING.get(self.config.model, {'input': 1.0, 'output': 3.0})
        
        input_cost = (token_usage['input_tokens'] / 1_000_000) * pricing['input']
        output_cost = (token_usage['output_tokens'] / 1_000_000) * pricing['output']
        
        return round(input_cost + output_cost, 6)
    
    def _estimate_confidence(self, response: str) -> float:
        """
        Estimate confidence in the response.
        
        Based on presence of uncertainty markers.
        """
        uncertainty_markers = [
            'might', 'maybe', 'possibly', 'uncertain', 'not sure',
            'could be', 'might be', 'probably', 'unsure', '?'
        ]
        
        response_lower = response.lower()
        uncertainty_count = sum(1 for marker in uncertainty_markers if marker in response_lower)
        
        # Start with high confidence, reduce for each uncertainty marker
        confidence = max(0.5, 1.0 - (uncertainty_count * 0.1))
        
        return confidence
    
    def _log_execution(self, prompt: str, response: str, token_usage: Dict, cost: float):
        """Log execution for audit trail."""
        log_entry = {
            'agent': self.__class__.__name__,
            'provider': self.config.provider,
            'model': self.config.model,
            'prompt_length': len(prompt),
            'response_length': len(response),
            'token_usage': token_usage,
            'cost_usd': cost,
            'timestamp': time.time()
        }
        
        logger.info(f"Agent execution: {json.dumps(log_entry)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get agent statistics."""
        return {
            'total_calls': self.total_calls,
            'total_cost_usd': round(self.total_cost, 4),
            'avg_cost_per_call': round(self.total_cost / max(self.total_calls, 1), 4),
            'provider': self.config.provider,
            'model': self.config.model
        }
    
    # ==================== LANGCHAIN AGENT PATTERNS ====================
    # Following Databricks GenAI Course patterns
    
    def create_react_agent(self, tools: List[Any], prompt_template: str = None) -> Any:
        """
        Create a ReAct-style agent with tools.
        
        This follows the Databricks GenAI course pattern using:
        - create_tool_calling_agent
        - AgentExecutor
        
        Args:
            tools: List of LangChain tools
            prompt_template: Optional custom prompt template
        
        Returns:
            AgentExecutor instance
        
        Example:
            ```python
            from langchain.tools import Tool
            
            tools = [
                Tool(name="search", func=search_fn, description="Search for info")
            ]
            
            agent = my_agent.create_react_agent(tools)
            result = agent.invoke({"input": "What is X?"})
            ```
        """
        if not self.config.use_langchain:
            raise ValueError("ReAct agents require use_langchain=True in config")
        
        try:
            from langchain.agents import AgentExecutor, create_tool_calling_agent
            from langchain.prompts import PromptTemplate
            from langchain import hub
            
            # Use custom prompt or pull from LangChain Hub
            if prompt_template:
                prompt = PromptTemplate.from_template(prompt_template)
            else:
                # Use the standard ReAct prompt from LangChain Hub
                prompt = hub.pull("hwchase17/openai-functions-agent")
            
            # Create the agent
            agent = create_tool_calling_agent(self.client, tools, prompt)
            
            # Create executor with error handling
            agent_executor = AgentExecutor(
                agent=agent,
                tools=tools,
                verbose=True,
                handle_parsing_errors=True,
                max_iterations=5
            )
            
            logger.info(f"Created ReAct agent with {len(tools)} tools")
            return agent_executor
            
        except ImportError as e:
            logger.error(f"LangChain not installed: {e}")
            raise
    
    def register_to_unity_catalog(
        self,
        model_name: str,
        input_example: Any,
        signature: Any = None
    ) -> str:
        """
        Register the agent to Unity Catalog via MLflow.
        
        This follows the Databricks GenAI course pattern for
        model registration and deployment.
        
        Args:
            model_name: Full UC path (catalog.schema.model)
            input_example: Example input for signature inference
            signature: Optional MLflow signature
        
        Returns:
            Model URI
        
        Example:
            ```python
            model_uri = agent.register_to_unity_catalog(
                model_name="production.ml.code_converter_agent",
                input_example="def process()..."
            )
            ```
        """
        if not MLFLOW_AVAILABLE:
            raise ImportError("MLflow not installed. Install: pip install mlflow")
        
        import mlflow
        from mlflow.models import infer_signature
        
        # Set registry to Unity Catalog
        mlflow.set_registry_uri("databricks-uc")
        
        # Create wrapper function for the agent
        def agent_invoke(input_data: str) -> str:
            response = self.execute({'code': input_data})
            return response.output
        
        # Start MLflow run
        with mlflow.start_run(run_name=f"{self.__class__.__name__}_registration") as run:
            # Infer signature if not provided
            if signature is None:
                sample_output = self.execute({'code': input_example})
                signature = infer_signature(input_example, sample_output.output)
            
            # Log the model
            model_info = mlflow.pyfunc.log_model(
                python_model=agent_invoke,
                artifact_path="agent",
                registered_model_name=model_name,
                input_example=input_example,
                signature=signature
            )
            
            logger.info(f"Registered agent to {model_name}")
            return f"models:/{model_name}/{model_info.registered_model_version}"
    
    def invoke_with_langchain(self, input_data: Dict[str, Any]) -> AgentResponse:
        """
        Invoke agent using LangChain patterns.
        
        For simple LLM calls without tools, uses ChatDatabricks directly.
        """
        if not self.config.use_langchain:
            return self.execute(input_data)
        
        start_time = time.time()
        
        try:
            prompt = self._build_prompt(input_data)
            
            # Use ChatDatabricks .invoke()
            response = self.client.invoke(prompt)
            
            # Extract content
            if hasattr(response, 'content'):
                output = response.content
            else:
                output = str(response)
            
            # Parse response
            parsed = self._parse_response(output)
            
            return AgentResponse(
                success=True,
                output=parsed,
                reasoning=output,
                confidence=self._estimate_confidence(output),
                token_usage={},  # LangChain doesn't expose this easily
                cost_usd=0.0,
                duration_seconds=time.time() - start_time,
                errors=[],
                warnings=[],
                metadata={'mode': 'langchain'}
            )
            
        except Exception as e:
            logger.error(f"LangChain invocation failed: {e}")
            return AgentResponse(
                success=False,
                output='',
                reasoning='',
                confidence=0.0,
                token_usage={},
                cost_usd=0.0,
                duration_seconds=time.time() - start_time,
                errors=[str(e)],
                warnings=[],
                metadata={}
            )