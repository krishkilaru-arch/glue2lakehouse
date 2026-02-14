"""
Code Converter Agent
LLM-powered AWS Glue to Databricks code conversion

Uses AI to understand context and generate optimized Databricks code.

Follows Databricks GenAI patterns:
- ChatDatabricks for LangChain integration
- Optional ReAct agent with code tools
- MLflow autologging
- Unity Catalog model registration

Author: Analytics360
Version: 2.1.0
"""

from typing import Dict, Any, List, Optional
import json
import re
from glue2lakehouse.agents.base_agent import BaseAgent, AgentConfig, AgentResponse
import logging

logger = logging.getLogger(__name__)

# Define tools for ReAct agent mode
def get_code_converter_tools() -> List:
    """
    Get LangChain tools for code conversion agent.
    
    These tools enable ReAct-style autonomous reasoning.
    Following Databricks GenAI course patterns.
    """
    try:
        from langchain.tools import Tool
        from langchain_experimental.utilities import PythonREPL
        
        # Python REPL for code validation
        python_repl = PythonREPL()
        repl_tool = Tool(
            name="python_syntax_checker",
            description="Check Python syntax by attempting to compile code. Input should be Python code.",
            func=lambda code: _check_syntax(code)
        )
        
        # Glue pattern detector
        glue_detector = Tool(
            name="glue_pattern_detector",
            description="Detect AWS Glue patterns in code. Returns list of Glue-specific constructs found.",
            func=lambda code: _detect_glue_patterns(code)
        )
        
        # Databricks optimizer
        optimizer_tool = Tool(
            name="databricks_optimizer",
            description="Suggest Databricks optimizations for code. Input is Databricks PySpark code.",
            func=lambda code: _suggest_optimizations(code)
        )
        
        return [repl_tool, glue_detector, optimizer_tool]
        
    except ImportError:
        logger.warning("LangChain tools not available")
        return []


def _check_syntax(code: str) -> str:
    """Check Python syntax."""
    try:
        compile(code, '<string>', 'exec')
        return "✅ Syntax is valid"
    except SyntaxError as e:
        return f"❌ Syntax error: {e}"


def _detect_glue_patterns(code: str) -> str:
    """Detect Glue-specific patterns."""
    patterns = {
        'DynamicFrame': 'DynamicFrame usage - convert to DataFrame',
        'GlueContext': 'GlueContext - replace with SparkSession',
        'from_catalog': 'Glue Catalog read - use Unity Catalog',
        'write_dynamic_frame': 'DynamicFrame write - use DataFrame.write',
        'apply_mapping': 'ApplyMapping - use DataFrame select/withColumn',
        'getResolvedOptions': 'Job parameters - use dbutils.widgets'
    }
    
    found = []
    for pattern, description in patterns.items():
        if pattern in code:
            found.append(f"- {pattern}: {description}")
    
    return "\n".join(found) if found else "No Glue patterns detected"


def _suggest_optimizations(code: str) -> str:
    """Suggest Databricks optimizations."""
    suggestions = []
    
    if '.write.mode(' in code and 'delta' not in code.lower():
        suggestions.append("- Use Delta format: .format('delta')")
    
    if 'spark.read' in code and 'spark.read.table' not in code:
        suggestions.append("- Consider using spark.read.table() for Unity Catalog")
    
    if 'repartition' in code:
        suggestions.append("- Consider liquid clustering instead of manual repartitioning")
    
    if 'cache()' in code:
        suggestions.append("- Delta caching may be more efficient than .cache()")
    
    return "\n".join(suggestions) if suggestions else "Code looks optimized"


class CodeConverterAgent(BaseAgent):
    """
    AI agent for converting Glue code to Databricks.
    
    Supports two modes following Databricks GenAI patterns:
    
    1. Direct SDK mode (default):
       - Uses databricks-sdk WorkspaceClient
       - Cost tracking and retry logic
       - Efficient for batch processing
    
    2. LangChain mode (use_langchain=True):
       - Uses ChatDatabricks from langchain_databricks
       - MLflow autologging enabled
       - ReAct agent with tools
       - Can be registered to Unity Catalog
    
    Advantages over rule-based:
    - Understands context and intent
    - Handles complex transformations
    - Suggests optimizations
    - Adapts to code style
    - Explains changes
    
    Example (Direct SDK):
        ```python
        config = AgentConfig(provider='databricks', model='databricks-meta-llama-3-3-70b-instruct')
        agent = CodeConverterAgent(config)
        
        result = agent.execute({
            'code': glue_code,
            'context': {'database': 'raw', 'table': 'customers'}
        })
        
        if result.success:
            print(result.output)  # Converted Databricks code
        ```
    
    Example (LangChain with ReAct agent):
        ```python
        # Following Databricks GenAI course patterns
        config = AgentConfig(
            provider='databricks',
            model='databricks-meta-llama-3-3-70b-instruct',
            use_langchain=True
        )
        agent = CodeConverterAgent(config)
        
        # Create ReAct agent with tools
        tools = get_code_converter_tools()
        react_agent = agent.create_react_agent(tools)
        
        # Run agent
        result = react_agent.invoke({"input": "Convert this Glue code: ..."})
        ```
    
    Example (Register to Unity Catalog):
        ```python
        model_uri = agent.register_to_unity_catalog(
            model_name="production.ml.code_converter_agent",
            input_example="def process(): pass"
        )
        ```
    """
    
    SYSTEM_PROMPT = """You are an expert data engineer specializing in AWS Glue to Databricks migrations.

Your task is to convert AWS Glue PySpark code to Databricks PySpark code.

Key Transformations:
1. DynamicFrame → DataFrame
2. GlueContext → SparkSession
3. glueContext.create_dynamic_frame → spark.read
4. Glue Data Catalog → Unity Catalog
5. S3 paths → External Locations or Delta tables
6. Job parameters → dbutils.widgets
7. Remove AWS Glue-specific imports
8. Add Databricks-specific optimizations

Rules:
- Maintain business logic exactly
- Use modern Databricks patterns
- Apply Delta Lake best practices
- Add liquid clustering suggestions where applicable
- Use Unity Catalog 3-level namespace (catalog.schema.table)
- Keep code clean and readable
- Add comments explaining major changes

Output Format:
Return ONLY valid Python code. No markdown, no explanations in the code.
After the code, add a section starting with "### EXPLANATION:" to explain changes."""
    
    def _build_prompt(self, input_data: Dict[str, Any]) -> str:
        """Build conversion prompt."""
        code = input_data.get('code', '')
        context = input_data.get('context', {})
        
        prompt = f"""Convert this AWS Glue code to Databricks code:

```python
{code}
```

Context:
{json.dumps(context, indent=2)}

Requirements:
1. Convert DynamicFrames to DataFrames
2. Replace GlueContext with SparkSession
3. Use Unity Catalog (catalog.schema.table format)
4. Apply Delta Lake optimizations
5. Remove AWS Glue imports
6. Add dbutils where appropriate
7. Maintain exact business logic
8. Add type hints and docstrings

Return:
1. Complete converted code (valid Python)
2. Explanation of changes (after "### EXPLANATION:")
"""
        
        return prompt
    
    def _parse_response(self, response: str) -> str:
        """Parse LLM response to extract code."""
        # Split on explanation marker
        if "### EXPLANATION:" in response:
            code, explanation = response.split("### EXPLANATION:", 1)
        else:
            code = response
            explanation = ""
        
        # Remove markdown code blocks if present
        code = self._extract_code_from_markdown(code)
        
        # Clean up
        code = code.strip()
        
        return code
    
    def _extract_code_from_markdown(self, text: str) -> str:
        """Extract code from markdown code blocks."""
        # Match ```python ... ``` or ``` ... ```
        pattern = r'```(?:python)?\n(.*?)\n```'
        matches = re.findall(pattern, text, re.DOTALL)
        
        if matches:
            return matches[0]
        
        return text
    
    def convert_file(self, file_path: str) -> AgentResponse:
        """
        Convert entire Glue file to Databricks.
        
        Args:
            file_path: Path to Glue Python file
        
        Returns:
            AgentResponse with converted code
        """
        with open(file_path, 'r') as f:
            code = f.read()
        
        return self.execute({
            'code': code,
            'context': {'file_path': file_path}
        })
    
    def convert_function(
        self,
        function_code: str,
        function_name: str,
        input_schema: Dict[str, str] = None
    ) -> AgentResponse:
        """
        Convert a single function.
        
        Args:
            function_code: Function code to convert
            function_name: Name of the function
            input_schema: Expected input schema
        
        Returns:
            AgentResponse with converted function
        """
        context = {
            'function_name': function_name,
            'conversion_type': 'function'
        }
        
        if input_schema:
            context['input_schema'] = input_schema
        
        return self.execute({
            'code': function_code,
            'context': context
        })
    
    def explain_conversion(self, glue_code: str, databricks_code: str) -> str:
        """
        Generate explanation of conversion changes.
        
        Args:
            glue_code: Original Glue code
            databricks_code: Converted Databricks code
        
        Returns:
            Explanation text
        """
        prompt = f"""Compare these two code snippets and explain what changed:

Original AWS Glue code:
```python
{glue_code}
```

Converted Databricks code:
```python
{databricks_code}
```

Provide:
1. List of key changes
2. Reasons for each change
3. Databricks best practices applied
4. Any potential issues or improvements
"""
        
        response, _ = self._call_llm(prompt)
        return response


class HybridConverter:
    """
    Hybrid converter that uses both rule-based and LLM-based approaches.
    
    Strategy:
    1. Try rule-based conversion first (fast, no cost)
    2. If confidence is low, use LLM for difficult parts
    3. Use LLM to validate and optimize
    
    Example:
        ```python
        converter = HybridConverter(rule_based_migrator, llm_agent)
        
        result = converter.convert(glue_code)
        # Uses rules where possible, LLM where needed
        ```
    """
    
    def __init__(self, rule_based_migrator, llm_agent: CodeConverterAgent):
        """
        Initialize hybrid converter.
        
        Args:
            rule_based_migrator: Rule-based migrator (existing)
            llm_agent: LLM-based agent (new)
        """
        self.rule_based = rule_based_migrator
        self.llm_agent = llm_agent
        self.stats = {
            'total_conversions': 0,
            'rule_based_count': 0,
            'llm_based_count': 0,
            'hybrid_count': 0,
            'total_cost': 0.0
        }
    
    def convert(
        self,
        code: str,
        strategy: str = 'hybrid'
    ) -> Dict[str, Any]:
        """
        Convert code using chosen strategy.
        
        Args:
            code: Glue code to convert
            strategy: 'rule', 'llm', or 'hybrid'
        
        Returns:
            Conversion result
        """
        self.stats['total_conversions'] += 1
        
        if strategy == 'rule':
            return self._convert_rule_based(code)
        elif strategy == 'llm':
            return self._convert_llm_based(code)
        else:  # hybrid
            return self._convert_hybrid(code)
    
    def _convert_rule_based(self, code: str) -> Dict[str, Any]:
        """Convert using rule-based approach."""
        self.stats['rule_based_count'] += 1
        
        # Use existing rule-based migrator
        result = self.rule_based.transform_code(code)
        
        return {
            'success': True,
            'code': result,
            'method': 'rule_based',
            'confidence': 0.8,  # Rules are predictable
            'cost': 0.0
        }
    
    def _convert_llm_based(self, code: str) -> Dict[str, Any]:
        """Convert using LLM."""
        self.stats['llm_based_count'] += 1
        
        result = self.llm_agent.execute({'code': code, 'context': {}})
        
        self.stats['total_cost'] += result.cost_usd
        
        return {
            'success': result.success,
            'code': result.output,
            'method': 'llm_based',
            'confidence': result.confidence,
            'cost': result.cost_usd,
            'reasoning': result.reasoning
        }
    
    def _convert_hybrid(self, code: str) -> Dict[str, Any]:
        """Convert using hybrid approach."""
        self.stats['hybrid_count'] += 1
        
        # Step 1: Try rule-based first
        rule_result = self._convert_rule_based(code)
        
        # Step 2: Assess complexity
        complexity = self._assess_complexity(code)
        
        # Step 3: If complex, use LLM to validate/improve
        if complexity > 0.7:  # High complexity threshold
            logger.info("High complexity detected, using LLM to improve conversion")
            
            # Use LLM to improve
            llm_result = self.llm_agent.execute({
                'code': rule_result['code'],
                'context': {
                    'original': code,
                    'note': 'This code was converted using rules. Please review and optimize.'
                }
            })
            
            self.stats['total_cost'] += llm_result.cost_usd
            
            return {
                'success': llm_result.success,
                'code': llm_result.output,
                'method': 'hybrid',
                'confidence': (rule_result['confidence'] + llm_result.confidence) / 2,
                'cost': llm_result.cost_usd,
                'reasoning': llm_result.reasoning
            }
        else:
            # Rule-based result is sufficient
            return {
                **rule_result,
                'method': 'hybrid',
                'note': 'Low complexity - rule-based conversion sufficient'
            }
    
    def _assess_complexity(self, code: str) -> float:
        """
        Assess code complexity (0.0 to 1.0).
        
        Factors:
        - Number of DynamicFrame operations
        - Complex transformations (joins, windows, UDFs)
        - Custom logic
        - Error handling
        """
        complexity_score = 0.0
        
        # DynamicFrame usage (harder to convert)
        dynamic_frame_count = code.lower().count('dynamicframe')
        complexity_score += min(dynamic_frame_count * 0.1, 0.3)
        
        # Complex operations
        complex_ops = ['apply_mapping', 'relationalize', 'join', 'window']
        for op in complex_ops:
            if op in code.lower():
                complexity_score += 0.1
        
        # Custom UDFs
        if 'def ' in code and '@udf' in code:
            complexity_score += 0.2
        
        # Error handling (more complex)
        if 'try:' in code or 'except' in code:
            complexity_score += 0.1
        
        # Lines of code
        lines = code.split('\n')
        if len(lines) > 100:
            complexity_score += 0.2
        
        return min(complexity_score, 1.0)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get conversion statistics."""
        return {
            **self.stats,
            'cost_per_conversion': self.stats['total_cost'] / max(self.stats['total_conversions'], 1)
        }
