# 📦 Package Distribution Guide

## What Gets Included in PyPI Package

### ✅ Included Files

**Core Package**:
- `glue2lakehouse/` - Complete Python package
  - All `.py` files
  - All `__init__.py` files
  - `*.yaml` and `*.yml` configuration files

**Documentation**:
- `README.md` - Project overview and quick start
- `LICENSE` - MIT License
- `requirements.txt` - Dependencies

### ❌ Excluded from PyPI Package

**Folders** (kept in Git, not in PyPI):
- `docs/` - Full documentation (use GitHub or ReadTheDocs)
- `tests/` - Test suite
- `examples/` - Usage examples
- `recycle/` - Archived files
- `databricks_app/` - Databricks App (deploy separately)
- `.github/` - CI/CD workflows

**Files**:
- All `*.md` files EXCEPT `README.md`
- Development config: `pytest.ini`, `mypy.ini`, `.flake8`
- Git files: `.gitignore`, `.cursorrules`
- Build artifacts: `__pycache__`, `*.pyc`, `*.pyo`

---

## Why This Split?

### PyPI Package Goals:
- **Small size**: Faster pip install
- **Essential only**: Just the code users need
- **Clean**: No development artifacts

### Git Repository Goals:
- **Complete**: All documentation and examples
- **Development**: Tests, CI/CD, configs
- **Reference**: Full project history

---

## File Sizes

**PyPI Package**: ~500 KB (Python code only)
**Git Repository**: ~5 MB (includes docs, tests, examples)

**Benefit**: 10x smaller package for users!

---

## Configuration Files

### `setup.py`
```python
packages=find_packages(exclude=[
    "tests", "tests.*",
    "examples", "examples.*",
    "docs", "docs.*",
    "recycle", "recycle.*",
    "databricks_app", "databricks_app.*"
])
```

### `MANIFEST.in`
```ini
# Include
include README.md
include LICENSE
include requirements.txt
recursive-include glue2lakehouse *.yaml *.yml

# Exclude
prune docs
prune tests
prune examples
prune recycle
prune databricks_app
global-exclude *.md
include README.md  # Exception
```

---

## User Access to Full Resources

Users can still access everything via:

**GitHub Repository**:
```bash
git clone https://github.com/krishkilaru-arch/glue2lakehouse
cd glue2lakehouse
```

**Documentation Site** (future):
```
https://glue2lakehouse.readthedocs.io
```

**Examples**:
```
https://github.com/krishkilaru-arch/glue2lakehouse/tree/main/examples
```

---

## Testing the Package Build

```bash
# Build the package
python setup.py sdist bdist_wheel

# Check what's included
tar -tzf dist/glue2lakehouse-*.tar.gz | head -30

# Verify exclusions
tar -tzf dist/glue2lakehouse-*.tar.gz | grep -E "docs/|examples/|tests/"
# Should return nothing
```

---

## Publishing to PyPI

```bash
# Install twine
pip install twine

# Build
python setup.py sdist bdist_wheel

# Check package
twine check dist/*

# Upload to TestPyPI first
twine upload --repository testpypi dist/*

# If all good, upload to PyPI
twine upload dist/*
```

---

**Result**: Clean, professional PyPI package with full resources on GitHub! ✨

**Version**: 2.0.0  
**Last Updated**: 2026-02-13
