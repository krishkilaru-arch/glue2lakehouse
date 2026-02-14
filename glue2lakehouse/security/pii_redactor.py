"""
PII Redactor for AI Code Analysis
Removes sensitive data before sending code to AI agents

CRITICAL for compliance (GDPR, CCPA, HIPAA)

Author: Analytics360
Version: 2.0.0
"""

import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class RedactionResult:
    """Result of PII redaction."""
    redacted_code: str
    redactions: List[Dict[str, str]]
    redaction_map: Dict[str, str]  # For reversing redactions
    is_safe: bool
    warnings: List[str]


class PIIRedactor:
    """
    Redacts PII and sensitive data from code before AI analysis.
    
    Removes:
    - AWS credentials (access keys, secret keys)
    - Connection strings
    - API keys
    - Email addresses
    - IP addresses
    - Hardcoded passwords
    - S3 bucket names with PII
    - Database credentials
    - SSN, credit cards (if in comments/strings)
    
    Example:
        ```python
        redactor = PIIRedactor()
        
        result = redactor.redact(source_code)
        
        if result.is_safe:
            # Safe to send to AI
            ai_response = agent.convert(result.redacted_code)
            
            # Restore original values
            restored_code = redactor.restore(ai_response, result.redaction_map)
        ```
    """
    
    # Patterns for PII detection
    PATTERNS = {
        'aws_access_key': (
            r'AKIA[0-9A-Z]{16}',
            'AWS_ACCESS_KEY_REDACTED'
        ),
        'aws_secret_key': (
            r'["\']?[A-Za-z0-9/+=]{40}["\']?',
            'AWS_SECRET_KEY_REDACTED'
        ),
        'api_key': (
            r'api[_-]?key["\']?\s*[:=]\s*["\']?([a-zA-Z0-9_\-]{20,})',
            'API_KEY_REDACTED'
        ),
        'password': (
            r'password["\']?\s*[:=]\s*["\']?([^"\'\s]+)',
            'PASSWORD_REDACTED'
        ),
        'email': (
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'EMAIL_REDACTED'
        ),
        'ip_address': (
            r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
            'IP_REDACTED'
        ),
        'ssn': (
            r'\b\d{3}-\d{2}-\d{4}\b',
            'SSN_REDACTED'
        ),
        'credit_card': (
            r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
            'CC_REDACTED'
        ),
        'jdbc_connection': (
            r'jdbc:[^"\'\s]+',
            'JDBC_URL_REDACTED'
        ),
        's3_bucket': (
            r's3://[a-z0-9][a-z0-9\-]*[a-z0-9]',
            'S3_BUCKET_REDACTED'
        ),
        'bearer_token': (
            r'Bearer\s+[A-Za-z0-9\-_\.]+',
            'BEARER_TOKEN_REDACTED'
        ),
        'private_key': (
            r'-----BEGIN (?:RSA )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA )?PRIVATE KEY-----',
            'PRIVATE_KEY_REDACTED'
        )
    }
    
    # High-risk keywords that should trigger warnings
    SENSITIVE_KEYWORDS = [
        'ssn', 'social_security', 'credit_card', 'password', 'secret',
        'private_key', 'api_key', 'token', 'credential', 'confidential',
        'pii', 'phi', 'hipaa', 'gdpr'
    ]
    
    def __init__(self, strict_mode: bool = True):
        """
        Initialize redactor.
        
        Args:
            strict_mode: If True, redact aggressively. If False, more permissive.
        """
        self.strict_mode = strict_mode
        logger.info(f"PIIRedactor initialized (strict_mode={strict_mode})")
    
    def redact(self, code: str) -> RedactionResult:
        """
        Redact PII from code.
        
        Args:
            code: Source code to redact
        
        Returns:
            RedactionResult with redacted code and metadata
        """
        logger.info("Starting PII redaction")
        
        redacted_code = code
        redactions = []
        redaction_map = {}
        warnings = []
        
        # Apply each pattern
        for pii_type, (pattern, replacement) in self.PATTERNS.items():
            matches = re.finditer(pattern, redacted_code, re.IGNORECASE)
            
            for match in matches:
                original_value = match.group(0)
                
                # Create unique placeholder
                placeholder = f"<<<{replacement}_{len(redactions)}>>>"
                
                # Store redaction
                redactions.append({
                    'type': pii_type,
                    'original': original_value,
                    'placeholder': placeholder,
                    'position': match.span()
                })
                
                redaction_map[placeholder] = original_value
                
                # Replace in code
                redacted_code = redacted_code.replace(original_value, placeholder)
                
                logger.warning(f"Redacted {pii_type}: {original_value[:10]}...")
        
        # Check for sensitive keywords
        for keyword in self.SENSITIVE_KEYWORDS:
            if keyword.lower() in code.lower():
                warnings.append(
                    f"Found sensitive keyword '{keyword}' - manual review recommended"
                )
        
        # Determine if safe
        is_safe = self._assess_safety(code, redactions, warnings)
        
        logger.info(f"Redaction complete: {len(redactions)} items redacted, is_safe={is_safe}")
        
        return RedactionResult(
            redacted_code=redacted_code,
            redactions=redactions,
            redaction_map=redaction_map,
            is_safe=is_safe,
            warnings=warnings
        )
    
    def restore(self, code: str, redaction_map: Dict[str, str]) -> str:
        """
        Restore original values after AI processing.
        
        Args:
            code: Code with placeholders
            redaction_map: Map from placeholders to original values
        
        Returns:
            Code with original values restored
        """
        restored_code = code
        
        for placeholder, original in redaction_map.items():
            restored_code = restored_code.replace(placeholder, original)
        
        return restored_code
    
    def _assess_safety(
        self,
        code: str,
        redactions: List[Dict],
        warnings: List[str]
    ) -> bool:
        """
        Assess if code is safe to send to AI after redaction.
        
        Returns:
            True if safe, False if manual review needed
        """
        # If strict mode and we found PII, require manual review
        if self.strict_mode and redactions:
            return False
        
        # If high-risk keywords found
        if len(warnings) > 3:
            return False
        
        # Check for unredacted patterns
        dangerous_patterns = [
            r'password\s*=\s*["\'][^"\']+["\']',
            r'secret\s*=\s*["\'][^"\']+["\']',
            r'token\s*=\s*["\'][^"\']+["\']'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                return False
        
        return True
    
    def redact_table_names(self, code: str, sensitive_tables: List[str]) -> str:
        """
        Redact specific table names.
        
        Args:
            code: Source code
            sensitive_tables: List of table names to redact
        
        Returns:
            Code with table names redacted
        """
        redacted = code
        
        for table in sensitive_tables:
            # Match various formats: table_name, "table_name", 'table_name'
            patterns = [
                table,
                f'"{table}"',
                f"'{table}'",
                f'`{table}`'
            ]
            
            for pattern in patterns:
                placeholder = f"<<<TABLE_{hash(table) % 10000}>>>"
                redacted = redacted.replace(pattern, placeholder)
        
        return redacted
    
    def sanitize_for_logging(self, code: str) -> str:
        """
        Sanitize code for safe logging (doesn't need to be reversible).
        
        Args:
            code: Source code
        
        Returns:
            Sanitized code safe for logs
        """
        result = self.redact(code)
        return result.redacted_code


class ComplianceChecker:
    """
    Checks code against compliance requirements.
    
    GDPR, CCPA, HIPAA, SOC2, etc.
    """
    
    def __init__(self):
        """Initialize compliance checker."""
        self.regulations = {
            'GDPR': self._check_gdpr,
            'CCPA': self._check_ccpa,
            'HIPAA': self._check_hipaa,
            'SOC2': self._check_soc2
        }
    
    def check(self, code: str, regulations: List[str] = None) -> Dict[str, Any]:
        """
        Check code against compliance regulations.
        
        Args:
            code: Source code
            regulations: List of regulations to check (default: all)
        
        Returns:
            Compliance report
        """
        regulations = regulations or list(self.regulations.keys())
        
        report = {
            'compliant': True,
            'violations': [],
            'recommendations': []
        }
        
        for regulation in regulations:
            if regulation in self.regulations:
                check_func = self.regulations[regulation]
                result = check_func(code)
                
                if not result['compliant']:
                    report['compliant'] = False
                    report['violations'].extend(result['violations'])
                    report['recommendations'].extend(result['recommendations'])
        
        return report
    
    def _check_gdpr(self, code: str) -> Dict[str, Any]:
        """Check GDPR compliance."""
        violations = []
        
        # Check for PII in code
        if re.search(r'(email|phone|address|name)', code, re.IGNORECASE):
            violations.append({
                'regulation': 'GDPR',
                'violation': 'Potential PII found in code',
                'severity': 'high',
                'article': 'Article 5(1)(f)'
            })
        
        # Check for data retention
        if 'DELETE' not in code.upper() and 'DROP' not in code.upper():
            violations.append({
                'regulation': 'GDPR',
                'violation': 'No data deletion mechanism found',
                'severity': 'medium',
                'article': 'Article 17 (Right to erasure)'
            })
        
        return {
            'compliant': len(violations) == 0,
            'violations': violations,
            'recommendations': [
                'Implement data minimization',
                'Add data retention policies',
                'Ensure encryption at rest and in transit'
            ]
        }
    
    def _check_ccpa(self, code: str) -> Dict[str, Any]:
        """Check CCPA compliance."""
        violations = []
        
        # Similar checks as GDPR
        if re.search(r'(email|phone|address)', code, re.IGNORECASE):
            violations.append({
                'regulation': 'CCPA',
                'violation': 'Personal information found in code',
                'severity': 'high'
            })
        
        return {
            'compliant': len(violations) == 0,
            'violations': violations,
            'recommendations': ['Implement consumer data rights']
        }
    
    def _check_hipaa(self, code: str) -> Dict[str, Any]:
        """Check HIPAA compliance."""
        violations = []
        
        # Check for PHI
        phi_indicators = ['patient', 'medical', 'health', 'diagnosis', 'ssn', 'dob']
        
        for indicator in phi_indicators:
            if indicator in code.lower():
                violations.append({
                    'regulation': 'HIPAA',
                    'violation': f'Potential PHI indicator: {indicator}',
                    'severity': 'critical'
                })
        
        return {
            'compliant': len(violations) == 0,
            'violations': violations,
            'recommendations': [
                'Encrypt all PHI',
                'Implement access controls',
                'Add audit logging'
            ]
        }
    
    def _check_soc2(self, code: str) -> Dict[str, Any]:
        """Check SOC2 compliance."""
        violations = []
        
        # Check for hardcoded credentials
        if re.search(r'(password|secret|key)\s*=\s*["\'][^"\']+["\']', code, re.IGNORECASE):
            violations.append({
                'regulation': 'SOC2',
                'violation': 'Hardcoded credentials detected',
                'severity': 'critical',
                'control': 'CC6.1'
            })
        
        return {
            'compliant': len(violations) == 0,
            'violations': violations,
            'recommendations': [
                'Use secret management service',
                'Implement key rotation',
                'Add encryption'
            ]
        }


# Utility functions
def is_safe_for_ai(code: str, strict: bool = True) -> Tuple[bool, List[str]]:
    """
    Quick check if code is safe to send to AI.
    
    Args:
        code: Source code
        strict: Strict mode
    
    Returns:
        (is_safe, warnings)
    """
    redactor = PIIRedactor(strict_mode=strict)
    result = redactor.redact(code)
    
    return result.is_safe, result.warnings


def redact_and_send_to_ai(code: str, ai_function) -> str:
    """
    Safely redact, send to AI, and restore.
    
    Args:
        code: Source code
        ai_function: Function that sends to AI
    
    Returns:
        AI response with original values restored
    """
    redactor = PIIRedactor()
    
    # Redact
    result = redactor.redact(code)
    
    if not result.is_safe:
        raise ValueError(
            f"Code not safe for AI analysis. Warnings: {result.warnings}"
        )
    
    # Send to AI
    ai_response = ai_function(result.redacted_code)
    
    # Restore
    restored = redactor.restore(ai_response, result.redaction_map)
    
    return restored
