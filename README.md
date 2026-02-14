# Glue2Lakehouse

> **An Intelligent AWS Glue → Databricks Lakehouse Migration Accelerator**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

## 🚀 What is Glue2Lakehouse?

Glue2Lakehouse is an **enterprise-grade, AI-powered migration framework** that automates 75-85% of AWS Glue to Databricks migrations. It combines rule-based code transformation with semantic validation, workflow migration, and comprehensive metadata tracking.

### Key Features

- ✅ **Automated Code Conversion**: Transform DynamicFrames → DataFrames, GlueContext → SparkSession
- ✅ **Semantic Validation**: Verify migrated code produces identical results
- ✅ **Workflow Migration**: Convert multi-job Glue Workflows to Databricks Workflows
- ✅ **Dual-Track Development**: Sync changes between live Glue and Databricks codebases
- ✅ **PII/Security**: Redact sensitive data before AI analysis (GDPR/CCPA/HIPAA compliant)
- ✅ **Dependency Analysis**: Detect circular dependencies, calculate migration order
- ✅ **Multi-Project Support**: Track 1-100+ projects via Delta tables
- ✅ **Executive Dashboard**: Databricks App for management visibility
- ✅ **Delta Lake Native**: All metadata stored in Unity Catalog tables

---

## 📖 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/glue2databricks.git
cd glue2databricks

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

### Basic Usage

```python
from glue2databricks import GlueMigrator

# Migrate a single Glue script
migrator = GlueMigrator()
result = migrator.migrate_file(
    source="glue_scripts/etl_job.py",
    target="databricks_scripts/etl_job.py"
)

print(f"✅ Migration complete! Complexity: {result.complexity}")
```

### Migrate an Entire Repository

```bash
# Migrate entire Git repository
python -m glue2databricks migrate-package \
  --source /path/to/glue/repo \
  --target /path/to/databricks/repo

# Incremental updates (only changed files)
python -m glue2databricks update \
  --source /path/to/glue/repo \
  --target /path/to/databricks/repo
```

---

## 🎯 Use Cases

### 1. **Simple ETL Migration**
- Convert standalone Glue jobs to Databricks notebooks
- 90%+ automation for simple transformations

### 2. **Complex Python Package Migration**
- Migrate entire Glue libraries (modules, functions, classes)
- Dependency analysis and migration ordering
- 70-80% automation

### 3. **Live Parallel Development**
- Keep Glue running while building Databricks version
- Sync changes from Glue → Databricks
- Protect Databricks-native code from overwrites

### 4. **Multi-Project Enterprise Migration**
- Track 100+ projects simultaneously
- Delta-based metadata for scalability
- Executive dashboard for progress visibility

---

## 📚 Documentation

### 🚀 Quick Start Guides
- [**5-Minute Quickstart**](docs/quickstart/QUICKSTART.md) - Get started in 5 minutes
- [**Repository Migration**](docs/quickstart/QUICKSTART_REPO.md) - Migrate entire Git repos
- [**Entity Tracking**](docs/quickstart/QUICKSTART_ENTITY_TRACKING.md) - Track source/destination entities
- [**Incremental Updates**](docs/quickstart/INCREMENTAL_QUICKSTART.md) - Update only changed files
- [**SDK Reference**](docs/quickstart/SDK_QUICK_REFERENCE.md) - Python SDK cheat sheet
- [**Getting Started**](docs/quickstart/GETTING_STARTED.md) - Step-by-step tutorial

### 📖 Comprehensive Guides
- [**Usage Guide**](docs/guides/USAGE.md) - Detailed feature walkthrough
- [**Package Migration**](docs/guides/PACKAGE_MIGRATION_GUIDE.md) - Migrate Python packages
- [**Repository Migration**](docs/guides/REPO_MIGRATION_GUIDE.md) - Full repo migration
- [**Incremental Migration**](docs/guides/INCREMENTAL_MIGRATION.md) - Sync strategies
- [**Dual-Track Development**](docs/guides/DUAL_TRACK_GUIDE.md) - Parallel Glue + Databricks
- [**Entity Tracking**](docs/guides/ENTITY_TRACKING_GUIDE.md) - Metadata tracking
- [**Production Guide**](docs/guides/PRODUCTION_README.md) - Production deployment
- [**Production Migration**](docs/guides/PRODUCTION_MIGRATION_GUIDE.md) - Enterprise patterns
- [**Databricks Dashboard**](docs/guides/DATABRICKS_DASHBOARD_DEPLOYMENT.md) - Deploy Streamlit app

### 🏗️ Architecture
- [**Glue2Lakehouse Architecture**](docs/architecture/GLUE2LAKEHOUSE_ARCHITECTURE.md) - Complete system design
- [**Project Summary**](docs/architecture/PROJECT_SUMMARY.md) - Technical overview

### 📊 Results & Summaries
- [**Complete Solution**](docs/summaries/COMPLETE_SOLUTION_SUMMARY.md) - Full feature list
- [**Production Features**](docs/summaries/PRODUCTION_FEATURES_SUMMARY.md) - Enterprise features
- [**Production Ready**](docs/summaries/PRODUCTION_READY_SUMMARY.md) - Readiness assessment
- [**Package Support**](docs/summaries/PACKAGE_SUPPORT_SUMMARY.md) - Package migration results
- [**Demo Complete**](docs/summaries/DEMO_COMPLETE.md) - Migration demo results
- [**Migration Results**](docs/summaries/MIGRATION_DEMO_RESULTS.md) - Detailed metrics
- [**Sample Project**](docs/summaries/SAMPLE_PROJECT_GUIDE.md) - Sample migration guide

### 💡 Specific Solutions
- [**Your Use Case**](docs/answers/YOUR_USE_CASE_SOLUTION.md) - Risk engine parallel migration
- [**Incremental Migration Answer**](docs/answers/ANSWER_YOUR_QUESTION.md) - Incremental sync solution
- [**Sample Project Answer**](docs/answers/ANSWER_SAMPLE_PROJECT.md) - Sample project details

---

## 🏗️ Architecture

```
User Input (Git Repo URL, Project Name)
         ↓
Migration Orchestrator (Multi-Project Coordination)
         ↓
    ┌────┴────┬────────┐
    │         │        │
Git Extractor  DDL     Catalog
(Clone+Parse)  Migrator Scanner
    │         │        │
    └────┬────┴────────┘
         ↓
Delta Metadata Tables (migration_projects, source_entities,
                       destination_entities, validation_results)
         ↓
    ┌────┴────┬────────────┬────────────┐
    │         │            │            │
Code      Validation  Optimization  Drift
Converter  Agent       Agent        Detector
Agent
    │         │            │            │
    └────┬────┴────────────┴────────────┘
         ↓
Databricks Outputs (Unity Catalog, Repos, Delta Tables, Jobs)
         ↓
Databricks App Dashboard (Multi-Project View, Progress, Alerts)
```

---

## 🔑 Key Components

### Core Migration Engine
- **Code Transformer**: AST-based Python code transformation
- **API Mapper**: Glue → Databricks API mappings
- **DDL Migrator**: CREATE TABLE statement conversion

### Validation Framework
- **Semantic Validator**: Verify output equivalence (row counts, schemas, aggregations)
- **Dependency Analyzer**: Detect circular dependencies, calculate migration order
- **Schema Drift Detector**: Compare Glue Catalog vs Unity Catalog

### Workflow & Orchestration
- **Workflow Migrator**: Convert Glue Workflows → Databricks Workflows (DAGs)
- **Multi-Project Orchestrator**: Manage 1-100+ concurrent migrations
- **Incremental Migrator**: Sync only changed files

### Security & Compliance
- **PII Redactor**: Remove sensitive data before AI analysis
- **Compliance Checker**: GDPR, CCPA, HIPAA, SOC2 validation
- **Audit Logger**: Track all migration decisions in Delta

### Tracking & Visualization
- **Entity Tracker**: Track modules, functions, classes (source & destination)
- **Table Tracker**: Track table schemas, detect drift
- **Databricks Dashboard**: Executive-level migration status

### Enterprise Features
- **Dual-Track Manager**: Parallel Glue + Databricks development
- **Python SDK**: Programmatic API for custom tools
- **Plugin System**: Extensible architecture
- **Backup & Rollback**: Safe migration with undo capability

---

## 🎯 Automation Target

| Component | Automation Level |
|-----------|------------------|
| DDL Conversion | 95% |
| Catalog Mapping | 90% |
| DynamicFrame → DataFrame | 85% |
| GlueContext removal | 90% |
| S3 → External Location | 85% |
| Complex UDF refactoring | 70% |
| boto3-heavy jobs | 60% |
| **Overall** | **75-85%** ✅ |

---

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=glue2databricks --cov-report=html

# Run specific test suite
pytest tests/unit/
pytest tests/integration/
```

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Built with [PySpark](https://spark.apache.org/docs/latest/api/python/)
- Powered by [Databricks](https://databricks.com/)
- AST manipulation via [ast](https://docs.python.org/3/library/ast.html)

---

## 📞 Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/your-org/glue2databricks/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/glue2databricks/discussions)

---

## 🚀 What's Next?

1. **Glue Bookmark Migration** - Convert bookmarks to Delta checkpoints
2. **Performance Benchmarking** - Automated before/after comparison
3. **Lineage Preservation** - Unity Catalog lineage tracking
4. **AI Agent Integration** - Databricks Agents for code conversion

---

**Built with ❤️ for the Databricks community**

*Transform your Glue workloads into modern Lakehouse pipelines* 🎯
