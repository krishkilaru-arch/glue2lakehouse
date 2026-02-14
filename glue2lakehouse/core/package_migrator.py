"""
Package and library migration support for Glue code integrated into Python packages
"""

import os
from pathlib import Path
from typing import Dict, List, Set
import ast

from glue2lakehouse.core.migrator import GlueMigrator
from glue2lakehouse.utils.logger import logger


class PackageMigrator(GlueMigrator):
    """
    Extended migrator for handling Glue code integrated into Python packages/libraries
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.module_dependencies = {}
        self.glue_modules = set()
        self.migration_order = []
    
    def migrate_package(self, input_package: str, output_package: str, 
                       preserve_structure: bool = True) -> Dict:
        """
        Migrate an entire Python package with Glue code
        
        Args:
            input_package: Path to input package directory
            output_package: Path to output package directory
            preserve_structure: Maintain directory structure
        
        Returns:
            Dictionary with migration results
        """
        logger.info(f"Migrating package: {input_package} → {output_package}")
        
        input_path = Path(input_package)
        output_path = Path(output_package)
        
        if not input_path.exists():
            return {
                'success': False,
                'error': f"Input package does not exist: {input_package}"
            }
        
        # Create output directory
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Step 1: Discover all Python files
        python_files = self._discover_python_files(input_path)
        logger.info(f"Found {len(python_files)} Python files in package")
        
        # Step 2: Analyze dependencies
        logger.info("Analyzing module dependencies...")
        self._analyze_package_dependencies(python_files, input_path)
        
        # Step 3: Identify Glue modules
        logger.info("Identifying Glue-dependent modules...")
        self._identify_glue_modules(python_files)
        
        # Step 4: Determine migration order (dependency-aware)
        logger.info("Computing migration order...")
        self._compute_migration_order()
        
        # Step 5: Migrate files in order
        results = []
        for py_file in self.migration_order:
            relative_path = py_file.relative_to(input_path)
            out_file = output_path / relative_path
            
            # Create subdirectories
            out_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Migrate the file
            result = self.migrate_file(str(py_file), str(out_file))
            results.append({
                'file': str(relative_path),
                'success': result['success'],
                'has_glue_code': str(py_file) in self.glue_modules,
                'error': result.get('error')
            })
            
            if result['success']:
                logger.info(f"✓ Migrated: {relative_path}")
            else:
                logger.error(f"✗ Failed: {relative_path} - {result.get('error')}")
        
        # Step 6: Copy/migrate __init__.py files
        self._handle_init_files(input_path, output_path)
        
        # Step 7: Generate migration report
        report = self._generate_package_report(results)
        
        successful = sum(1 for r in results if r['success'])
        
        return {
            'success': successful == len(results),
            'total_files': len(results),
            'successful': successful,
            'failed': len(results) - successful,
            'glue_modules': len(self.glue_modules),
            'results': results,
            'report': report
        }
    
    def _discover_python_files(self, package_path: Path) -> List[Path]:
        """Discover all Python files in package"""
        return list(package_path.rglob('*.py'))
    
    def _analyze_package_dependencies(self, python_files: List[Path], 
                                     package_root: Path):
        """Analyze import dependencies between modules"""
        for py_file in python_files:
            try:
                with open(py_file, 'r') as f:
                    source = f.read()
                
                tree = ast.parse(source)
                imports = self._extract_imports(tree)
                
                # Store dependencies
                self.module_dependencies[str(py_file)] = imports
                
            except Exception as e:
                logger.warning(f"Failed to analyze {py_file}: {e}")
    
    def _extract_imports(self, tree: ast.AST) -> List[str]:
        """Extract all imports from AST"""
        imports = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        return imports
    
    def _identify_glue_modules(self, python_files: List[Path]):
        """Identify which modules contain Glue code"""
        glue_indicators = [
            'awsglue',
            'GlueContext',
            'DynamicFrame',
            'Job.init',
            'getResolvedOptions'
        ]
        
        for py_file in python_files:
            try:
                with open(py_file, 'r') as f:
                    content = f.read()
                
                # Check if file contains Glue-specific code
                if any(indicator in content for indicator in glue_indicators):
                    self.glue_modules.add(str(py_file))
                    logger.debug(f"Glue code detected in: {py_file.name}")
            
            except Exception as e:
                logger.warning(f"Failed to read {py_file}: {e}")
    
    def _compute_migration_order(self):
        """Compute migration order based on dependencies"""
        # For now, simple topological sort
        # Migrate files with Glue code first, then others
        
        all_files = set(self.module_dependencies.keys())
        glue_files = self.glue_modules
        non_glue_files = all_files - glue_files
        
        # Glue files first (they're likely the main logic)
        self.migration_order = [Path(f) for f in glue_files]
        # Then non-Glue files
        self.migration_order.extend([Path(f) for f in non_glue_files])
    
    def _handle_init_files(self, input_path: Path, output_path: Path):
        """Handle __init__.py files in the package"""
        init_files = input_path.rglob('__init__.py')
        
        # Glue-related patterns that need migration
        glue_patterns = [
            'awsglue',
            'GlueContext',
            'glue_context',
            'init_glue_context',
            'DynamicFrame',
            'GlueContextManager'
        ]
        
        for init_file in init_files:
            relative_path = init_file.relative_to(input_path)
            out_init = output_path / relative_path
            
            try:
                with open(init_file, 'r') as f:
                    content = f.read()
                
                # Check if __init__.py has any Glue-related content
                has_glue_content = any(pattern in content for pattern in glue_patterns)
                
                if has_glue_content:
                    # Migrate it using full transformation
                    result = self.migrate_file(str(init_file), str(out_init))
                    if result['success']:
                        logger.info(f"Migrated __init__.py: {relative_path}")
                else:
                    # Just copy it
                    out_init.parent.mkdir(parents=True, exist_ok=True)
                    with open(out_init, 'w') as f:
                        f.write(content)
                    logger.info(f"Copied __init__.py: {relative_path}")
            
            except Exception as e:
                logger.warning(f"Failed to handle {init_file}: {e}")
    
    def _generate_package_report(self, results: List[Dict]) -> str:
        """Generate a detailed migration report"""
        report = []
        report.append("=" * 70)
        report.append("PACKAGE MIGRATION REPORT")
        report.append("=" * 70)
        report.append("")
        
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        glue_count = sum(1 for r in results if r.get('has_glue_code'))
        
        report.append(f"Total Files: {len(results)}")
        report.append(f"Successful: {successful}")
        report.append(f"Failed: {failed}")
        report.append(f"Files with Glue code: {glue_count}")
        report.append("")
        
        if glue_count > 0:
            report.append("Files containing Glue code:")
            for r in results:
                if r.get('has_glue_code'):
                    status = "✓" if r['success'] else "✗"
                    report.append(f"  {status} {r['file']}")
            report.append("")
        
        if failed > 0:
            report.append("Failed migrations:")
            for r in results:
                if not r['success']:
                    report.append(f"  ✗ {r['file']}")
                    if r.get('error'):
                        report.append(f"    Error: {r['error']}")
            report.append("")
        
        report.append("Next Steps:")
        report.append("1. Review all migrated files, especially those with Glue code")
        report.append("2. Update import statements if package name changed")
        report.append("3. Test module imports and dependencies")
        report.append("4. Update setup.py or pyproject.toml if applicable")
        report.append("5. Run your test suite")
        report.append("")
        report.append("=" * 70)
        
        return "\n".join(report)
    
    def analyze_package(self, package_path: str) -> Dict:
        """
        Analyze a Python package without migrating
        
        Args:
            package_path: Path to package directory
        
        Returns:
            Analysis results
        """
        logger.info(f"Analyzing package: {package_path}")
        
        input_path = Path(package_path)
        if not input_path.exists():
            return {
                'success': False,
                'error': f"Package does not exist: {package_path}"
            }
        
        # Discover files
        python_files = self._discover_python_files(input_path)
        
        # Analyze dependencies
        self._analyze_package_dependencies(python_files, input_path)
        
        # Identify Glue modules
        self._identify_glue_modules(python_files)
        
        # Analyze each Glue module
        detailed_analysis = []
        for glue_file in self.glue_modules:
            analysis = super().analyze_file(glue_file)
            if analysis['success']:
                detailed_analysis.append({
                    'file': Path(glue_file).relative_to(input_path),
                    'complexity': analysis['complexity_score'],
                    'dynamic_frames': analysis['dynamic_frame_count'],
                    'catalog_refs': len(analysis['catalog_references']),
                    'transforms': len(analysis['transforms_used'])
                })
        
        return {
            'success': True,
            'total_files': len(python_files),
            'glue_files': len(self.glue_modules),
            'non_glue_files': len(python_files) - len(self.glue_modules),
            'glue_modules': [str(Path(f).relative_to(input_path)) 
                            for f in self.glue_modules],
            'detailed_analysis': detailed_analysis,
            'total_complexity': sum(a['complexity'] for a in detailed_analysis)
        }
