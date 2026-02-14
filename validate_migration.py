#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    GLUE2LAKEHOUSE MIGRATION VALIDATOR                        ║
║                                                                              ║
║  Validates migrated code WITHOUT needing Databricks!                         ║
║  Works locally with: Offline (free), OpenAI, or Anthropic                    ║
╚══════════════════════════════════════════════════════════════════════════════╝

USAGE:
    # Option 1: Offline (No API needed - rule-based validation)
    python validate_migration.py

    # Option 2: With OpenAI (requires API key)
    export OPENAI_API_KEY="sk-..."
    python validate_migration.py --provider openai

    # Option 3: With Anthropic/Claude (requires API key)
    export ANTHROPIC_API_KEY="sk-ant-..."
    python validate_migration.py --provider anthropic

    # Custom paths
    python validate_migration.py --source /path/to/glue --target /path/to/databricks

    # Save results to JSON
    python validate_migration.py --output results.json
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import ast
import re
from dataclasses import dataclass, asdict
from datetime import datetime


# ============================================================================
# TERMINAL COLORS (works on Mac/Linux/Windows 10+)
# ============================================================================
class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
    
    @classmethod
    def disable(cls):
        """Disable colors for non-terminal output."""
        cls.GREEN = cls.YELLOW = cls.RED = cls.BLUE = cls.CYAN = cls.BOLD = cls.RESET = ""


# ============================================================================
# DATA CLASSES
# ============================================================================
@dataclass
class ValidationIssue:
    type: str  # "error", "warning", "info"
    description: str
    line: int = 0
    suggestion: str = ""


@dataclass 
class FileValidationResult:
    file: str
    passed: bool
    confidence: float
    issues: List[Dict]
    databricks_patterns_found: List[str]
    glue_patterns_remaining: List[str]
    recommendations: List[str]
    summary: str


# ============================================================================
# RULE-BASED VALIDATOR (No API needed)
# ============================================================================
class RuleBasedValidator:
    """
    Validates migrations using pattern matching rules.
    No external API needed - works completely offline!
    """
    
    # Glue patterns that should NOT be in migrated code
    GLUE_PATTERNS = [
        (r'from awsglue', "awsglue import found"),
        (r'import awsglue', "awsglue import found"),
        (r'GlueContext\s*\(', "GlueContext instantiation"),
        (r'\.create_dynamic_frame\.from_catalog', "create_dynamic_frame.from_catalog not converted"),
        (r'\.create_dynamic_frame\.from_options', "create_dynamic_frame.from_options not converted"),
        (r'\.write_dynamic_frame\.', "write_dynamic_frame not converted"),
        (r'getResolvedOptions\s*\(', "getResolvedOptions not converted to dbutils.widgets"),
        (r':\s*DynamicFrame(?!\w)', "DynamicFrame type annotation"),
        (r'->\s*DynamicFrame(?!\w)', "DynamicFrame return type"),
        (r'\.toDF\s*\(\s*\)', ".toDF() should be removed"),
        (r'DynamicFrame\.fromDF', "DynamicFrame.fromDF should be removed"),
        (r'job\.commit\s*\(\s*\)', "job.commit() should be removed"),
        (r'job\.init\s*\(', "job.init() should be removed"),
        (r'= Job\s*\(', "Job class instantiation"),
        (r'ResolveChoice\.apply', "ResolveChoice.apply not converted"),
        (r'ApplyMapping\.apply', "ApplyMapping.apply not converted"),
    ]
    
    # Databricks patterns that SHOULD be in migrated code
    DATABRICKS_PATTERNS = [
        (r'SparkSession', "SparkSession"),
        (r'spark\.table\s*\(', "spark.table() for catalog"),
        (r'spark\.read\.format', "spark.read.format()"),
        (r'dbutils\.widgets', "dbutils.widgets for parameters"),
        (r'\.format\s*\(\s*["\']delta["\']', "Delta format"),
        (r':\s*DataFrame', "DataFrame type hints"),
        (r'/Volumes/', "Databricks Volumes paths"),
    ]
    
    def validate_file(self, original: str, converted: str, filename: str) -> FileValidationResult:
        """Validate a single file conversion."""
        issues = []
        glue_remaining = []
        databricks_found = []
        
        # 1. Syntax Check
        try:
            ast.parse(converted)
        except SyntaxError as e:
            issues.append({
                "type": "error",
                "description": f"Syntax error: {e.msg}",
                "line": e.lineno or 0,
                "suggestion": "Fix syntax error before deployment"
            })
            return FileValidationResult(
                file=filename,
                passed=False,
                confidence=1.0,
                issues=issues,
                databricks_patterns_found=[],
                glue_patterns_remaining=["SYNTAX_ERROR"],
                recommendations=["Fix syntax errors first"],
                summary="❌ FAILED - Syntax error"
            )
        
        # 2. Check for remaining Glue patterns
        in_docstring = False
        for i, line in enumerate(converted.split('\n'), 1):
            stripped = line.strip()
            
            # Track docstrings
            if stripped.startswith('"""') or stripped.startswith("'''"):
                if stripped.count('"""') == 2 or stripped.count("'''") == 2:
                    continue  # Single-line docstring
                in_docstring = not in_docstring
                continue
            
            # Skip comments and docstrings
            if stripped.startswith('#') or in_docstring:
                continue
            
            for pattern, description in self.GLUE_PATTERNS:
                if re.search(pattern, line):
                    issues.append({
                        "type": "error",
                        "description": description,
                        "line": i,
                        "suggestion": f"Convert {description.split()[0]} to Databricks equivalent"
                    })
                    if description not in glue_remaining:
                        glue_remaining.append(description)
        
        # 3. Check for Databricks patterns (positive validation)
        for pattern, name in self.DATABRICKS_PATTERNS:
            if re.search(pattern, converted):
                databricks_found.append(name)
        
        # 4. Function comparison
        original_funcs = set(re.findall(r'def (\w+)\s*\(', original))
        converted_funcs = set(re.findall(r'def (\w+)\s*\(', converted))
        
        # Check for renamed functions (expected)
        renamed = {
            'init_glue_context': 'init_spark_session',
        }
        
        for orig, expected in renamed.items():
            if orig in original_funcs and expected not in converted_funcs and orig not in converted_funcs:
                issues.append({
                    "type": "warning", 
                    "description": f"Function '{orig}' should be renamed to '{expected}'",
                    "line": 0,
                    "suggestion": f"Rename {orig} to {expected}"
                })
        
        # Check for missing functions (excluding renamed)
        for func in original_funcs:
            if func not in converted_funcs and func not in renamed:
                issues.append({
                    "type": "warning",
                    "description": f"Function '{func}' from original not found",
                    "line": 0,
                    "suggestion": "Verify function was intentionally removed or renamed"
                })
        
        # 5. Class comparison
        original_classes = set(re.findall(r'class (\w+)', original))
        converted_classes = set(re.findall(r'class (\w+)', converted))
        
        class_renames = {
            'GlueContextManager': 'DatabricksJobRunner',
        }
        
        for orig, expected in class_renames.items():
            if orig in original_classes:
                if expected in converted_classes:
                    databricks_found.append(f"Class renamed: {orig} → {expected}")
                elif orig in converted_classes:
                    issues.append({
                        "type": "warning",
                        "description": f"Class '{orig}' should be renamed to '{expected}'",
                        "line": 0,
                        "suggestion": f"Rename class to {expected}"
                    })
        
        # 6. Calculate confidence and result
        error_count = len([i for i in issues if i["type"] == "error"])
        warning_count = len([i for i in issues if i["type"] == "warning"])
        
        if error_count > 0:
            confidence = max(0.3, 1.0 - (error_count * 0.2))
            passed = False
            summary = f"❌ FAILED - {error_count} errors, {warning_count} warnings"
        elif warning_count > 3:
            confidence = 0.75
            passed = True
            summary = f"⚠️ PASSED with warnings - {warning_count} warnings"
        elif warning_count > 0:
            confidence = 0.9
            passed = True
            summary = f"✅ PASSED - {warning_count} minor warnings"
        else:
            confidence = 0.98
            passed = True
            summary = "✅ PASSED - Clean conversion"
        
        # Recommendations
        recommendations = []
        if not passed:
            recommendations.append("Fix all errors before deployment")
        if warning_count > 0:
            recommendations.append("Review warnings and verify intentional changes")
        if databricks_found:
            recommendations.append("Databricks patterns detected - good!")
        recommendations.append("Test with sample data before production")
        
        return FileValidationResult(
            file=filename,
            passed=passed,
            confidence=confidence,
            issues=issues,
            databricks_patterns_found=databricks_found,
            glue_patterns_remaining=glue_remaining,
            recommendations=recommendations,
            summary=summary
        )


# ============================================================================
# LLM-BASED VALIDATOR (OpenAI/Anthropic)
# ============================================================================
class LLMValidator:
    """
    Validates migrations using LLM for deeper semantic analysis.
    Requires OpenAI, Anthropic, or Databricks API credentials.
    """
    
    def __init__(self, provider: str):
        self.provider = provider
        self._validate_api_key()
    
    def _validate_api_key(self):
        """Check if required API key is set."""
        if self.provider == "openai":
            if not os.getenv("OPENAI_API_KEY"):
                raise ValueError(
                    "OPENAI_API_KEY not set!\n"
                    "Run: export OPENAI_API_KEY='sk-...'\n"
                    "Or use --provider offline for rule-based validation"
                )
        elif self.provider == "anthropic":
            if not os.getenv("ANTHROPIC_API_KEY"):
                raise ValueError(
                    "ANTHROPIC_API_KEY not set!\n"
                    "Run: export ANTHROPIC_API_KEY='sk-ant-...'\n"
                    "Or use --provider offline for rule-based validation"
                )
        elif self.provider == "databricks":
            if not os.getenv("DATABRICKS_HOST") or not os.getenv("DATABRICKS_TOKEN"):
                raise ValueError(
                    "DATABRICKS_HOST and DATABRICKS_TOKEN not set!\n"
                    "Run:\n"
                    "  export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'\n"
                    "  export DATABRICKS_TOKEN='dapi...'\n"
                    "Or use --provider offline for rule-based validation"
                )
    
    def validate_file(self, original: str, converted: str, filename: str) -> FileValidationResult:
        """Validate using LLM."""
        prompt = self._build_prompt(original, converted, filename)
        
        try:
            if self.provider == "openai":
                result = self._call_openai(prompt)
            elif self.provider == "databricks":
                result = self._call_databricks(prompt)
            else:
                result = self._call_anthropic(prompt)
            
            return FileValidationResult(
                file=filename,
                passed=result.get("validation_passed", False),
                confidence=result.get("confidence", 0.0),
                issues=result.get("issues", []),
                databricks_patterns_found=result.get("databricks_patterns", []),
                glue_patterns_remaining=result.get("glue_patterns", []),
                recommendations=result.get("recommendations", []),
                summary=result.get("summary", "LLM validation complete")
            )
        except Exception as e:
            return FileValidationResult(
                file=filename,
                passed=False,
                confidence=0.0,
                issues=[{"type": "error", "description": f"LLM error: {str(e)}", "line": 0}],
                databricks_patterns_found=[],
                glue_patterns_remaining=[],
                recommendations=["LLM validation failed - try offline mode"],
                summary=f"❌ LLM Error: {str(e)}"
            )
    
    def _build_prompt(self, original: str, converted: str, filename: str) -> str:
        return f"""You are a code migration validator specializing in AWS Glue to Databricks migrations.

**File: {filename}**

**Original AWS Glue Code:**
```python
{original[:4000]}
```

**Converted Databricks Code:**
```python
{converted[:4000]}
```

Analyze this conversion and respond with ONLY valid JSON (no markdown, no explanation):

{{
  "validation_passed": true,
  "confidence": 0.95,
  "issues": [
    {{"type": "error|warning|info", "description": "Issue description", "line": 0}}
  ],
  "databricks_patterns": ["SparkSession", "spark.table()"],
  "glue_patterns": [],
  "recommendations": ["Test with sample data"],
  "summary": "Brief summary of validation result"
}}

Check for:
1. All awsglue imports removed
2. GlueContext → SparkSession
3. DynamicFrame → DataFrame  
4. create_dynamic_frame.from_catalog → spark.table()
5. getResolvedOptions → dbutils.widgets
6. S3 paths → Volumes paths
7. Logic equivalence preserved"""
    
    def _call_openai(self, prompt: str) -> Dict:
        import openai
        
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    
    def _call_anthropic(self, prompt: str) -> Dict:
        import anthropic
        
        client = anthropic.Anthropic()
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}]
        )
        
        text = response.content[0].text
        # Extract JSON from response
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            return json.loads(json_match.group())
        raise ValueError("No valid JSON in response")
    
    def _call_databricks(self, prompt: str) -> Dict:
        """Call Databricks Foundation Models API."""
        import urllib.request
        import urllib.error
        
        host = os.getenv("DATABRICKS_HOST").rstrip("/")
        token = os.getenv("DATABRICKS_TOKEN")
        
        # Use custom Databricks endpoint
        endpoint = f"{host}/serving-endpoints/glue2lakehouse-endpoint/invocations"
        
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": prompt + "\n\nRespond with ONLY valid JSON, no markdown."
                }
            ],
            "max_tokens": 2048,
            "temperature": 0.1
        }
        
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(payload).encode('utf-8'),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
        )
        
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                # Extract content from Databricks response
                if "choices" in result and len(result["choices"]) > 0:
                    text = result["choices"][0].get("message", {}).get("content", "")
                else:
                    text = result.get("content", str(result))
                
                # Parse JSON from response
                json_match = re.search(r'\{[\s\S]*\}', text)
                if json_match:
                    return json.loads(json_match.group())
                raise ValueError("No valid JSON in Databricks response")
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            raise ValueError(f"Databricks API error ({e.code}): {error_body}")
        except urllib.error.URLError as e:
            raise ValueError(f"Connection error: {e.reason}")


# ============================================================================
# MAIN VALIDATION RUNNER
# ============================================================================
def run_validation(source_dir: str, target_dir: str, provider: str = "offline") -> Dict:
    """Run validation on all migrated files."""
    
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    
    if not source_path.exists():
        print(f"{Colors.RED}Error: Source directory not found: {source_dir}{Colors.RESET}")
        sys.exit(1)
    
    if not target_path.exists():
        print(f"{Colors.RED}Error: Target directory not found: {target_dir}{Colors.RESET}")
        sys.exit(1)
    
    # Initialize validator
    if provider == "offline":
        validator = RuleBasedValidator()
        print(f"{Colors.CYAN}Using: Rule-based validation (offline){Colors.RESET}")
    else:
        try:
            validator = LLMValidator(provider)
            print(f"{Colors.CYAN}Using: {provider.upper()} LLM validation{Colors.RESET}")
        except ValueError as e:
            print(f"{Colors.RED}{e}{Colors.RESET}")
            sys.exit(1)
    
    print()
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "source": str(source_path),
        "target": str(target_path),
        "provider": provider,
        "total_files": 0,
        "passed": 0,
        "failed": 0,
        "warnings": 0,
        "files": []
    }
    
    # Find and validate Python files
    for target_file in sorted(target_path.rglob("*.py")):
        rel_path = target_file.relative_to(target_path)
        source_file = source_path / rel_path
        
        if not source_file.exists():
            continue
        
        results["total_files"] += 1
        
        original = source_file.read_text()
        converted = target_file.read_text()
        
        # Validate
        result = validator.validate_file(original, converted, str(rel_path))
        
        # Print result
        if result.passed:
            results["passed"] += 1
            status_color = Colors.GREEN
        else:
            results["failed"] += 1
            status_color = Colors.RED
        
        warning_count = len([i for i in result.issues if i.get("type") == "warning"])
        results["warnings"] += warning_count
        
        print(f"{Colors.BLUE}{rel_path}{Colors.RESET}")
        print(f"   {status_color}{result.summary}{Colors.RESET} (confidence: {result.confidence:.0%})")
        
        # Show issues
        for issue in result.issues[:3]:
            issue_color = Colors.RED if issue.get("type") == "error" else Colors.YELLOW
            line_info = f" (line {issue['line']})" if issue.get('line') else ""
            print(f"   {issue_color}• {issue['description']}{line_info}{Colors.RESET}")
        
        if len(result.issues) > 3:
            print(f"   {Colors.YELLOW}... and {len(result.issues) - 3} more issues{Colors.RESET}")
        
        # Store result
        results["files"].append(asdict(result) if hasattr(result, '__dataclass_fields__') else {
            "file": result.file,
            "passed": result.passed,
            "confidence": result.confidence,
            "issues": result.issues,
            "summary": result.summary
        })
        print()
    
    return results


def print_summary(results: Dict):
    """Print validation summary."""
    total = results["total_files"]
    passed = results["passed"]
    failed = results["failed"]
    warnings = results["warnings"]
    
    success_rate = (passed / total * 100) if total > 0 else 0
    
    print(f"""
{Colors.BOLD}{'═' * 70}
                         VALIDATION SUMMARY
{'═' * 70}{Colors.RESET}

   {Colors.BLUE}Total Files:{Colors.RESET}    {total}
   {Colors.GREEN}Passed:{Colors.RESET}         {passed}
   {Colors.RED}Failed:{Colors.RESET}         {failed}
   {Colors.YELLOW}Warnings:{Colors.RESET}       {warnings}
   
   {Colors.BOLD}Success Rate:{Colors.RESET}   {success_rate:.0f}%

{Colors.BOLD}{'═' * 70}{Colors.RESET}
""")
    
    if failed == 0:
        print(f"   {Colors.GREEN}✅ All validations passed! Ready for testing.{Colors.RESET}")
    else:
        print(f"   {Colors.RED}❌ {failed} file(s) need attention before deployment.{Colors.RESET}")
    
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Validate Glue to Databricks migration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_migration.py                              # Offline validation
  python validate_migration.py --provider openai            # Use OpenAI GPT-4
  python validate_migration.py --provider anthropic         # Use Claude
  python validate_migration.py --output results.json        # Save results
        """
    )
    
    parser.add_argument(
        "--source", 
        default="examples/sample_glue_repo",
        help="Source Glue repository path"
    )
    parser.add_argument(
        "--target", 
        default="examples/migrated_databricks_repo",
        help="Target Databricks repository path"
    )
    parser.add_argument(
        "--provider", 
        choices=["offline", "openai", "anthropic", "databricks"],
        default="offline",
        help="Validation provider (default: offline)"
    )
    parser.add_argument(
        "--output", 
        help="Output JSON file for detailed results"
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colored output"
    )
    
    args = parser.parse_args()
    
    if args.no_color:
        Colors.disable()
    
    # Header
    print(f"""
{Colors.BOLD}╔══════════════════════════════════════════════════════════════════════════════╗
║                    GLUE2LAKEHOUSE MIGRATION VALIDATOR                        ║
╠══════════════════════════════════════════════════════════════════════════════╣{Colors.RESET}
║  Source:   {args.source:<64} ║
║  Target:   {args.target:<64} ║
║  Provider: {args.provider:<64} ║
{Colors.BOLD}╚══════════════════════════════════════════════════════════════════════════════╝{Colors.RESET}
""")
    
    # Run validation
    results = run_validation(args.source, args.target, args.provider)
    
    # Print summary
    print_summary(results)
    
    # Save results if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"{Colors.CYAN}Results saved to: {args.output}{Colors.RESET}\n")
    
    # Exit code
    return 0 if results["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
