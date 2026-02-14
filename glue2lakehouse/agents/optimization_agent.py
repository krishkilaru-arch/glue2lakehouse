"""
Optimization Agent
LLM-powered code optimization suggestions

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any
from glue2lakehouse.agents.base_agent import BaseAgent
import logging

logger = logging.getLogger(__name__)


class OptimizationAgent(BaseAgent):
    """
    AI agent for suggesting Databricks optimizations.
    
    Suggests:
    - Delta Lake optimizations
    - Photon compatibility
    - Liquid clustering
    - Partition strategies
    - Caching opportunities
    - Broadcast joins
    
    Example:
        ```python
        agent = OptimizationAgent(config)
        result = agent.execute({'code': databricks_code})
        print(result.output)  # Optimization suggestions
        ```
    """
    
    def _build_prompt(self, input_data: Dict[str, Any]) -> str:
        """Build optimization prompt."""
        code = input_data.get('code', '')
        
        prompt = f"""Analyze this Databricks code and suggest optimizations:

```python
{code}
```

Please suggest optimizations for:
1. **Delta Lake**: Auto Optimize, Z-ORDER, Liquid Clustering
2. **Photon**: Ensure Photon compatibility
3. **Partitioning**: Optimal partition strategies
4. **Caching**: Where to cache DataFrames
5. **Joins**: Broadcast vs shuffle joins
6. **Performance**: Query optimization techniques

Return optimized code with comments explaining each optimization.
"""
        return prompt
    
    def _parse_response(self, response: str) -> str:
        """Parse optimization response."""
        # Extract code from markdown if present
        import re
        pattern = r'```(?:python)?\n(.*?)\n```'
        matches = re.findall(pattern, response, re.DOTALL)
        return matches[0] if matches else response
