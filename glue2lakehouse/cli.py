"""Command-line interface for Glue2Databricks"""

import click
import sys
from pathlib import Path
from glue2lakehouse.core.migrator import GlueMigrator
from glue2lakehouse.core.package_migrator import PackageMigrator
from glue2lakehouse.core.incremental_migrator import IncrementalMigrator
from glue2lakehouse.utils.logger import logger, setup_logger
from colorama import Fore, Style


@click.group()
@click.version_option(version='1.0.0')
def cli():
    """Glue2Databricks - Migrate AWS Glue code to Databricks"""
    pass


@cli.command()
@click.option('--input', '-i', 'input_path', required=True,
              help='Input Glue script file or directory')
@click.option('--output', '-o', 'output_path', required=True,
              help='Output Databricks script file or directory')
@click.option('--catalog', '-c', default='main',
              help='Target Unity Catalog name (default: main)')
@click.option('--config', type=click.Path(exists=True),
              help='Path to configuration file')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
@click.option('--no-notes', is_flag=True,
              help='Disable migration notes in output')
def migrate(input_path, output_path, catalog, config, verbose, no_notes):
    """
    Migrate Glue code to Databricks
    
    Examples:
        glue2databricks migrate -i glue_script.py -o databricks_script.py
        glue2databricks migrate -i ./glue_scripts/ -o ./databricks_scripts/
    """
    if verbose:
        setup_logger(level='DEBUG')
    
    # Print banner
    print_banner()
    
    # Initialize migrator
    migrator = GlueMigrator(
        target_catalog=catalog,
        config_path=config,
        add_notes=not no_notes,
        verbose=verbose
    )
    
    # Check if input is file or directory
    input_path_obj = Path(input_path)
    
    if not input_path_obj.exists():
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)
    
    try:
        if input_path_obj.is_file():
            # Migrate single file
            click.echo(f"\n{Fore.CYAN}Migrating file: {input_path}{Style.RESET_ALL}")
            result = migrator.migrate_file(input_path, output_path)
            
            if result['success']:
                print_success(f"Successfully migrated to: {output_path}")
                print_analysis_summary(result['analysis'])
                print_transformation_summary(result['transformations'])
            else:
                print_error(f"Migration failed: {result.get('error')}")
                sys.exit(1)
        
        elif input_path_obj.is_dir():
            # Migrate directory
            click.echo(f"\n{Fore.CYAN}Migrating directory: {input_path}{Style.RESET_ALL}")
            result = migrator.migrate_directory(input_path, output_path)
            
            if result['success'] or result['successful'] > 0:
                print_success(f"Migration complete!")
                print_directory_summary(result)
                
                if result['failed'] > 0:
                    logger.warning(f"{result['failed']} file(s) failed to migrate")
            else:
                print_error(f"Migration failed: {result.get('error')}")
                sys.exit(1)
        
        else:
            print_error(f"Invalid input path: {input_path}")
            sys.exit(1)
    
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option('--input', '-i', 'input_path', required=True,
              help='Input package directory')
@click.option('--output', '-o', 'output_path', required=True,
              help='Output package directory')
@click.option('--catalog', '-c', default='main',
              help='Target Unity Catalog name (default: main)')
@click.option('--config', type=click.Path(exists=True),
              help='Path to configuration file')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
@click.option('--report', '-r', 'report_path',
              help='Save detailed migration report to file')
def migrate_package(input_path, output_path, catalog, config, verbose, report_path):
    """
    Migrate a Python package with integrated Glue code
    
    This command is designed for Glue code integrated into Python libraries.
    It handles module dependencies and preserves package structure.
    
    Examples:
        glue2databricks migrate-package -i my_glue_lib/ -o my_databricks_lib/
        glue2databricks migrate-package -i src/etl/ -o dest/etl/ --report report.txt
    """
    if verbose:
        setup_logger(level='DEBUG')
    
    print_banner()
    
    # Initialize package migrator
    migrator = PackageMigrator(
        target_catalog=catalog,
        config_path=config,
        add_notes=True,
        verbose=verbose
    )
    
    click.echo(f"\n{Fore.CYAN}Migrating package: {input_path} → {output_path}{Style.RESET_ALL}\n")
    
    try:
        result = migrator.migrate_package(input_path, output_path)
        
        if result['success'] or result['successful'] > 0:
            print_success("Package migration complete!")
            print_package_summary(result)
            
            # Print report
            if 'report' in result:
                click.echo(f"\n{Fore.YELLOW}{result['report']}{Style.RESET_ALL}")
            
            # Save report to file if requested
            if report_path:
                with open(report_path, 'w') as f:
                    f.write(result['report'])
                print_success(f"Report saved to: {report_path}")
            
            if result['failed'] > 0:
                logger.warning(f"{result['failed']} file(s) failed to migrate")
        else:
            print_error(f"Package migration failed")
            sys.exit(1)
    
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option('--input', '-i', 'input_path', required=True,
              help='Input Glue script to analyze')
@click.option('--report', '-r', 'report_path',
              help='Output path for analysis report (optional)')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def analyze(input_path, report_path, verbose):
    """
    Analyze a Glue script without migrating
    
    Examples:
        glue2databricks analyze -i glue_script.py
        glue2databricks analyze -i glue_script.py -r analysis.txt
    """
    if verbose:
        setup_logger(level='DEBUG')
    
    print_banner()
    
    migrator = GlueMigrator()
    
    click.echo(f"\n{Fore.CYAN}Analyzing: {input_path}{Style.RESET_ALL}\n")
    
    try:
        result = migrator.analyze_file(input_path)
        
        if result['success']:
            print_analysis_details(result)
            
            # Write report if requested
            if report_path:
                write_analysis_report(result, report_path)
                print_success(f"Analysis report written to: {report_path}")
        else:
            print_error(f"Analysis failed: {result.get('error')}")
            sys.exit(1)
    
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option('--input', '-i', 'input_path', required=True,
              help='Input source directory (glue_source)')
@click.option('--output', '-o', 'output_path', required=True,
              help='Output target directory (databricks_target)')
@click.option('--files', '-f', 'specific_files', multiple=True,
              help='Specific files to update (can specify multiple)')
@click.option('--catalog', '-c', default='main',
              help='Target Unity Catalog name (default: main)')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def update(input_path, output_path, specific_files, catalog, verbose):
    """
    Update specific files - incremental migration
    
    This command allows you to re-migrate only changed files or specific files
    after the initial migration. Perfect for iterative development.
    
    Examples:
        # Update only changed files
        glue2databricks update -i glue_source/ -o databricks_target/
        
        # Update specific file
        glue2databricks update -i glue_source/ -o databricks_target/ -f src/readers.py
        
        # Update multiple files
        glue2databricks update -i glue_source/ -o databricks_target/ -f src/readers.py -f src/writers.py
    """
    if verbose:
        setup_logger(level='DEBUG')
    
    print_banner()
    
    migrator = IncrementalMigrator(
        target_catalog=catalog,
        verbose=verbose
    )
    
    click.echo(f"\n{Fore.CYAN}Incremental Update{Style.RESET_ALL}")
    click.echo(f"Source: {input_path}")
    click.echo(f"Target: {output_path}\n")
    
    try:
        if specific_files:
            click.echo(f"Updating specific files: {', '.join(specific_files)}\n")
            result = migrator.update_files(
                input_path, 
                output_path, 
                specific_files=list(specific_files)
            )
        else:
            click.echo("Detecting and updating changed files...\n")
            result = migrator.update_files(input_path, output_path)
        
        if result['success'] or result['updated'] > 0:
            print_success("Update complete!")
            print_update_summary(result)
        else:
            print_error(result.get('message', 'Update failed'))
            sys.exit(1)
    
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option('--input', '-i', 'input_path', required=True,
              help='Input source directory (glue_source)')
@click.option('--output', '-o', 'output_path',
              help='Output target directory (optional, for state check)')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def detect_changes(input_path, output_path, verbose):
    """
    Detect which files have changed since last migration
    
    Examples:
        glue2databricks detect-changes -i glue_source/ -o databricks_target/
        glue2databricks detect-changes -i glue_source/
    """
    if verbose:
        setup_logger(level='DEBUG')
    
    print_banner()
    
    migrator = IncrementalMigrator()
    
    click.echo(f"\n{Fore.CYAN}Detecting Changes{Style.RESET_ALL}")
    click.echo(f"Source: {input_path}\n")
    
    try:
        result = migrator.detect_changes(input_path, output_path)
        
        if result['success']:
            if result['count'] == 0:
                print_success("No changes detected - all files are up to date!")
            else:
                click.echo(f"{Fore.YELLOW}Changed Files:{Style.RESET_ALL}\n")
                for file in result['changed_files']:
                    click.echo(f"  • {file}")
                click.echo(f"\n{Fore.CYAN}Total: {result['count']} file(s) changed{Style.RESET_ALL}")
                click.echo(f"\nTo update these files, run:")
                click.echo(f"  glue2databricks update -i {input_path} -o databricks_target/")
        else:
            print_error("Failed to detect changes")
            sys.exit(1)
    
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option('--input', '-i', 'input_path', required=True,
              help='Input package directory to analyze')
@click.option('--report', '-r', 'report_path',
              help='Output path for analysis report')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def analyze_package(input_path, report_path, verbose):
    """
    Analyze a Python package with Glue code
    
    Examples:
        glue2databricks analyze-package -i my_glue_lib/
        glue2databricks analyze-package -i src/etl/ -r analysis.txt
    """
    if verbose:
        setup_logger(level='DEBUG')
    
    print_banner()
    
    migrator = PackageMigrator()
    
    click.echo(f"\n{Fore.CYAN}Analyzing package: {input_path}{Style.RESET_ALL}\n")
    
    try:
        result = migrator.analyze_package(input_path)
        
        if result['success']:
            print_package_analysis(result)
            
            # Write report if requested
            if report_path:
                write_package_analysis_report(result, report_path, input_path)
                print_success(f"Analysis report written to: {report_path}")
        else:
            print_error(f"Analysis failed: {result.get('error')}")
            sys.exit(1)
    
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def print_banner():
    """Print application banner"""
    banner = f"""
{Fore.CYAN}{'=' * 60}
  Glue2Databricks Migration Framework v1.0.0
  AWS Glue → Databricks Code Migration
{'=' * 60}{Style.RESET_ALL}
"""
    click.echo(banner)


def print_success(message):
    """Print success message"""
    click.echo(f"\n{Fore.GREEN}✓ {message}{Style.RESET_ALL}\n")


def print_error(message):
    """Print error message"""
    click.echo(f"\n{Fore.RED}✗ {message}{Style.RESET_ALL}\n", err=True)


def print_analysis_summary(analysis):
    """Print analysis summary"""
    click.echo(f"\n{Fore.YELLOW}Analysis Summary:{Style.RESET_ALL}")
    click.echo(f"  Complexity Score: {analysis.get('complexity_score', 0)}")
    click.echo(f"  DynamicFrames: {analysis.get('dynamic_frame_count', 0)}")
    click.echo(f"  Catalog References: {len(analysis.get('catalog_references', []))}")
    click.echo(f"  Transforms Used: {len(analysis.get('transforms_used', []))}")
    click.echo(f"  S3 Paths: {len(analysis.get('s3_paths', []))}")


def print_transformation_summary(transformations):
    """Print transformation summary"""
    if transformations:
        click.echo(f"\n{Fore.YELLOW}Transformations Applied:{Style.RESET_ALL}")
        for i, transform in enumerate(transformations[:10], 1):
            click.echo(f"  {i}. {transform}")
        if len(transformations) > 10:
            click.echo(f"  ... and {len(transformations) - 10} more")


def print_directory_summary(result):
    """Print directory migration summary"""
    click.echo(f"\n{Fore.YELLOW}Migration Summary:{Style.RESET_ALL}")
    click.echo(f"  Total Files: {result['total_files']}")
    click.echo(f"  {Fore.GREEN}Successful: {result['successful']}{Style.RESET_ALL}")
    if result['failed'] > 0:
        click.echo(f"  {Fore.RED}Failed: {result['failed']}{Style.RESET_ALL}")


def print_package_summary(result):
    """Print package migration summary"""
    click.echo(f"\n{Fore.YELLOW}Package Migration Summary:{Style.RESET_ALL}")
    click.echo(f"  Total Files: {result['total_files']}")
    click.echo(f"  Files with Glue Code: {result['glue_modules']}")
    click.echo(f"  {Fore.GREEN}Successful: {result['successful']}{Style.RESET_ALL}")
    if result['failed'] > 0:
        click.echo(f"  {Fore.RED}Failed: {result['failed']}{Style.RESET_ALL}")


def print_update_summary(result):
    """Print incremental update summary"""
    click.echo(f"\n{Fore.YELLOW}Update Summary:{Style.RESET_ALL}")
    click.echo(f"  {Fore.GREEN}Updated: {result['updated']}{Style.RESET_ALL}")
    if result.get('failed', 0) > 0:
        click.echo(f"  {Fore.RED}Failed: {result['failed']}{Style.RESET_ALL}")
    
    if result.get('files'):
        click.echo(f"\n{Fore.YELLOW}Files Updated:{Style.RESET_ALL}")
        for file_result in result['files']:
            status = "✓" if file_result['success'] else "✗"
            color = Fore.GREEN if file_result['success'] else Fore.RED
            click.echo(f"  {color}{status} {file_result['file']}{Style.RESET_ALL}")
            if file_result.get('error'):
                click.echo(f"    Error: {file_result['error']}")


def print_analysis_details(result):
    """Print detailed analysis results"""
    click.echo(f"{Fore.YELLOW}Analysis Results:{Style.RESET_ALL}\n")
    
    click.echo(f"Complexity Score: {Fore.CYAN}{result['complexity_score']}{Style.RESET_ALL}")
    click.echo(f"GlueContext Usage: {result['glue_context_usage']}")
    click.echo(f"DynamicFrame Operations: {result['dynamic_frame_count']}")
    
    if result['catalog_references']:
        click.echo(f"\n{Fore.YELLOW}Catalog References:{Style.RESET_ALL}")
        for ref in result['catalog_references']:
            click.echo(f"  • {ref['database']}.{ref['table']}")
    
    if result['transforms_used']:
        click.echo(f"\n{Fore.YELLOW}Glue Transforms Used:{Style.RESET_ALL}")
        for transform in set(result['transforms_used']):
            click.echo(f"  • {transform}")
    
    if result['s3_paths']:
        click.echo(f"\n{Fore.YELLOW}S3 Paths Found:{Style.RESET_ALL}")
        for path in result['s3_paths'][:5]:
            click.echo(f"  • {path}")
        if len(result['s3_paths']) > 5:
            click.echo(f"  ... and {len(result['s3_paths']) - 5} more")
    
    if result['job_arguments']:
        click.echo(f"\n{Fore.YELLOW}Job Arguments:{Style.RESET_ALL}")
        for arg in result['job_arguments']:
            click.echo(f"  • {arg}")


def print_package_analysis(result):
    """Print package analysis results"""
    click.echo(f"{Fore.YELLOW}Package Analysis Results:{Style.RESET_ALL}\n")
    
    click.echo(f"Total Python Files: {result['total_files']}")
    click.echo(f"Files with Glue Code: {Fore.CYAN}{result['glue_files']}{Style.RESET_ALL}")
    click.echo(f"Non-Glue Files: {result['non_glue_files']}")
    click.echo(f"Total Complexity: {result['total_complexity']}")
    
    if result['glue_modules']:
        click.echo(f"\n{Fore.YELLOW}Modules with Glue Code:{Style.RESET_ALL}")
        for module in result['glue_modules']:
            click.echo(f"  • {module}")
    
    if result['detailed_analysis']:
        click.echo(f"\n{Fore.YELLOW}Detailed Analysis:{Style.RESET_ALL}")
        for analysis in result['detailed_analysis']:
            click.echo(f"\n  {Fore.CYAN}{analysis['file']}{Style.RESET_ALL}")
            click.echo(f"    Complexity: {analysis['complexity']}")
            click.echo(f"    DynamicFrames: {analysis['dynamic_frames']}")
            click.echo(f"    Catalog Refs: {analysis['catalog_refs']}")
            click.echo(f"    Transforms: {analysis['transforms']}")


def write_analysis_report(result, report_path):
    """Write analysis report to file"""
    with open(report_path, 'w') as f:
        f.write("Glue2Databricks Analysis Report\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Complexity Score: {result['complexity_score']}\n")
        f.write(f"GlueContext Usage: {result['glue_context_usage']}\n")
        f.write(f"DynamicFrame Operations: {result['dynamic_frame_count']}\n\n")
        
        if result['catalog_references']:
            f.write("Catalog References:\n")
            for ref in result['catalog_references']:
                f.write(f"  - {ref['database']}.{ref['table']}\n")
            f.write("\n")
        
        if result['transforms_used']:
            f.write("Glue Transforms Used:\n")
            for transform in set(result['transforms_used']):
                f.write(f"  - {transform}\n")
            f.write("\n")
        
        if result['s3_paths']:
            f.write("S3 Paths:\n")
            for path in result['s3_paths']:
                f.write(f"  - {path}\n")
            f.write("\n")
        
        if result['imports']:
            f.write("Imports:\n")
            for imp in result['imports']:
                if imp['type'] == 'import':
                    f.write(f"  - import {imp['module']}\n")
                else:
                    f.write(f"  - from {imp['module']} import {imp['name']}\n")


def write_package_analysis_report(result, report_path, package_name):
    """Write package analysis report to file"""
    with open(report_path, 'w') as f:
        f.write("Glue2Databricks Package Analysis Report\n")
        f.write("=" * 70 + "\n\n")
        
        f.write(f"Package: {package_name}\n\n")
        f.write(f"Total Python Files: {result['total_files']}\n")
        f.write(f"Files with Glue Code: {result['glue_files']}\n")
        f.write(f"Non-Glue Files: {result['non_glue_files']}\n")
        f.write(f"Total Complexity Score: {result['total_complexity']}\n\n")
        
        if result['glue_modules']:
            f.write("Modules with Glue Code:\n")
            for module in result['glue_modules']:
                f.write(f"  - {module}\n")
            f.write("\n")
        
        if result['detailed_analysis']:
            f.write("Detailed Analysis per Module:\n")
            f.write("-" * 70 + "\n")
            for analysis in result['detailed_analysis']:
                f.write(f"\nFile: {analysis['file']}\n")
                f.write(f"  Complexity Score: {analysis['complexity']}\n")
                f.write(f"  DynamicFrame Operations: {analysis['dynamic_frames']}\n")
                f.write(f"  Catalog References: {analysis['catalog_refs']}\n")
                f.write(f"  Glue Transforms: {analysis['transforms']}\n")


if __name__ == '__main__':
    cli()
