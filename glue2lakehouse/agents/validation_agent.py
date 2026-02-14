"""
Validation Agent
LLM-powered validation of converted code

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any
from glue2lakehouse.agents.base_agent import BaseAgent
import logging

logger = logging.getLogger(__name__)


class ValidationAgent(BaseAgent):
    """
    AI agent for validating Glue → Databricks conversions.
    
    Validates:
    - Logic equivalence
    - Syntax correctness
    - Best practices
    - Performance implications
    - Security issues
    
    Example:
        ```python
        agent = ValidationAgent(config)
        result = agent.execute({
            'original_code': glue_code,
            'converted_code': databricks_code
        })
        
        if result.confidence > 0.9:
            print("✅ Validation passed")
        ```
    """
    
    def _build_prompt(self, input_data: Dict[str, Any]) -> str:
        """Build validation prompt."""
        original = input_data.get('original_code', '')
        converted = input_data.get('converted_code', '')
        
        prompt = f"""Validate this code conversion from AWS Glue to Databricks:

**Original AWS Glue Code:**
```python
{original}
```

**Converted Databricks Code:**
```python
{converted}
```

Please validate:
1. **Logic Equivalence**: Does converted code produce same results?
2. **Syntax**: Is the Databricks code syntactically correct?
3. **Best Practices**: Does it follow Databricks best practices?
4. **Performance**: Are there any performance concerns?
5. **Security**: Are there any security issues?

Respond in JSON format:
{{
  "validation_passed": true/false,
  "confidence": 0.0-1.0,
  "issues": [
    {{"type": "error/warning/info", "description": "...", "line": 0}}
  ],
  "recommendations": ["..."],
  "summary": "..."
}}
"""
        return prompt
    
    def _parse_response(self, response: str) -> str:
        """Parse validation response."""
        import json
        import re
        
        # Extract JSON from response
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except:
                pass
        
        return response
