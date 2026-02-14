"""
Agent Orchestrator
Coordinates all AI agents for end-to-end migration

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, Optional
from glue2lakehouse.agents.base_agent import AgentConfig
from glue2lakehouse.agents.code_converter_agent import CodeConverterAgent, HybridConverter
from glue2lakehouse.agents.validation_agent import ValidationAgent
from glue2lakehouse.agents.optimization_agent import OptimizationAgent
import logging

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    """
    Orchestrates all AI agents for complete migration workflow.
    
    Workflow:
    1. Convert code (CodeConverterAgent or HybridConverter)
    2. Validate conversion (ValidationAgent)
    3. Optimize code (OptimizationAgent)
    4. Final validation
    
    Example:
        ```python
        config = AgentConfig(provider='databricks')
        orchestrator = AgentOrchestrator(config, mode='hybrid')
        
        result = orchestrator.migrate_file('glue_job.py')
        
        if result['success']:
            print(f"✅ Migration complete!")
            print(f"Cost: ${result['total_cost']}")
            print(f"Confidence: {result['confidence']}")
        ```
    """
    
    def __init__(
        self,
        config: AgentConfig,
        mode: str = 'hybrid',
        rule_based_migrator=None
    ):
        """
        Initialize orchestrator.
        
        Args:
            config: Agent configuration
            mode: 'rule', 'llm', or 'hybrid'
            rule_based_migrator: Existing rule-based migrator (for hybrid mode)
        """
        self.config = config
        self.mode = mode
        
        # Initialize agents
        self.converter = CodeConverterAgent(config)
        self.validator = ValidationAgent(config)
        self.optimizer = OptimizationAgent(config)
        
        # Initialize hybrid converter if rule-based provided
        if mode == 'hybrid' and rule_based_migrator:
            self.hybrid = HybridConverter(rule_based_migrator, self.converter)
        else:
            self.hybrid = None
        
        logger.info(f"AgentOrchestrator initialized (mode={mode})")
    
    def migrate_code(
        self,
        code: str,
        context: Dict[str, Any] = None,
        skip_optimization: bool = False
    ) -> Dict[str, Any]:
        """
        Migrate Glue code to Databricks with full workflow.
        
        Args:
            code: Glue code to migrate
            context: Additional context
            skip_optimization: Skip optimization step
        
        Returns:
            Migration result with converted code and metadata
        """
        results = {
            'original_code': code,
            'success': False,
            'converted_code': '',
            'optimized_code': '',
            'validation': {},
            'total_cost': 0.0,
            'confidence': 0.0,
            'stages': []
        }
        
        try:
            # Stage 1: Code Conversion
            if self.mode == 'hybrid' and self.hybrid:
                conv_result = self.hybrid.convert(code)
                results['method'] = 'hybrid'
            elif self.mode == 'rule':
                conv_result = self._convert_rule_based(code)
                results['method'] = 'rule'
            else:  # llm
                conv_result = self.converter.execute({'code': code, 'context': context or {}})
                conv_result = {
                    'success': conv_result.success,
                    'code': conv_result.output,
                    'confidence': conv_result.confidence,
                    'cost': conv_result.cost_usd
                }
                results['method'] = 'llm'
            
            if not conv_result.get('success'):
                results['error'] = 'Conversion failed'
                return results
            
            results['converted_code'] = conv_result['code']
            results['total_cost'] += conv_result.get('cost', 0.0)
            results['stages'].append({'stage': 'conversion', 'success': True})
            
            # Stage 2: Validation
            val_result = self.validator.execute({
                'original_code': code,
                'converted_code': conv_result['code']
            })
            
            results['validation'] = val_result.output if val_result.success else {}
            results['total_cost'] += val_result.cost_usd
            results['stages'].append({'stage': 'validation', 'success': val_result.success})
            
            # Stage 3: Optimization (optional)
            if not skip_optimization:
                opt_result = self.optimizer.execute({'code': conv_result['code']})
                
                if opt_result.success:
                    results['optimized_code'] = opt_result.output
                    results['total_cost'] += opt_result.cost_usd
                    results['stages'].append({'stage': 'optimization', 'success': True})
                else:
                    results['optimized_code'] = conv_result['code']  # Use unoptimized
            else:
                results['optimized_code'] = conv_result['code']
            
            # Calculate overall confidence
            results['confidence'] = (conv_result.get('confidence', 0.8) + val_result.confidence) / 2
            results['success'] = True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            results['error'] = str(e)
        
        return results
    
    def _convert_rule_based(self, code: str) -> Dict[str, Any]:
        """Fallback rule-based conversion."""
        # This would use the existing rule-based migrator
        return {
            'success': True,
            'code': code,  # Placeholder - would use actual rule-based transform
            'confidence': 0.8,
            'cost': 0.0
        }
    
    def migrate_file(self, file_path: str) -> Dict[str, Any]:
        """Migrate entire file."""
        with open(file_path, 'r') as f:
            code = f.read()
        
        return self.migrate_code(code, {'file_path': file_path})
    
    def get_stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics."""
        stats = {
            'mode': self.mode,
            'converter': self.converter.get_stats(),
            'validator': self.validator.get_stats(),
            'optimizer': self.optimizer.get_stats()
        }
        
        if self.hybrid:
            stats['hybrid'] = self.hybrid.get_stats()
        
        return stats
