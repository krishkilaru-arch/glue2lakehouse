# Quick Start: Migrate Your Git Repository

## Your Use Case: Copy Git Repo → Migrate to databricks_target/

### Option 1: Automated Script (Easiest) ⭐

```bash
cd /Users/analytics360/glue2lakehouse

# Activate environment
source venv/bin/activate

# Run the migration script
./migrate_repo.sh /path/to/your/glue/repo production

# Or with a Git URL:
./migrate_repo.sh https://github.com/yourcompany/glue-repo.git production
```

**That's it!** Your code will be in `databricks_target/` folder. 🎉

---

### Option 2: Manual Steps

If you prefer to do it step-by-step:

```bash
cd /Users/analytics360/glue2lakehouse

# 1. Copy your Git repo to glue_source/
git clone <your-git-repo-url> glue_source/
# Or: cp -r /path/to/your/repo glue_source/

# 2. Activate environment
source venv/bin/activate

# 3. Analyze (optional but recommended)
python -m glue2lakehouse analyze-package \
    --input glue_source/ \
    --report analysis.txt

# 4. Run migration
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog main \
    --report migration_report.txt \
    --verbose

# 5. Review results
cat migration_report.txt
ls -la databricks_target/
```

---

## What Happens

### Before:
```
glue2lakehouse/
├── glue_source/              ← Your Glue code (copied from git)
│   ├── src/
│   │   ├── readers.py
│   │   ├── transformers.py
│   │   └── writers.py
│   ├── jobs/
│   │   ├── daily_etl.py
│   │   └── hourly_load.py
│   └── requirements.txt
```

### After:
```
glue2lakehouse/
├── glue_source/              ← Original Glue code (unchanged)
│   └── ...
├── databricks_target/        ← Migrated Databricks code ✨
│   ├── src/
│   │   ├── readers.py       # SparkSession instead of GlueContext
│   │   ├── transformers.py  # DataFrame instead of DynamicFrame
│   │   └── writers.py       # df.write instead of write_dynamic_frame
│   ├── jobs/
│   │   ├── daily_etl.py     # Migrated
│   │   └── hourly_load.py   # Migrated
│   └── requirements.txt
└── migration_report.txt      ← Detailed report
```

---

## Complete Example

```bash
# Navigate to framework
cd /Users/analytics360/glue2lakehouse

# Activate virtual environment
source venv/bin/activate

# Copy your Glue repo (choose one method):

# Method A: Clone from Git
git clone https://github.com/mycompany/etl-glue.git glue_source/

# Method B: Copy local repo
cp -r /Users/myuser/projects/my-glue-project glue_source/

# Method C: Symlink if you want to keep it in original location
ln -s /Users/myuser/projects/my-glue-project glue_source

# Run migration to databricks_target/
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog production \
    --report migration_report.txt \
    --verbose

# Check results
echo "✓ Migration complete!"
echo "Source: glue_source/"
echo "Target: databricks_target/"
cat migration_report.txt
```

---

## Using the Automated Script

The easiest way is to use the included script:

```bash
cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# For local repo:
./migrate_repo.sh /path/to/your/glue/repo main

# For Git URL:
./migrate_repo.sh https://github.com/company/repo.git production
```

**The script will:**
1. ✅ Clone/copy your repo to `glue_source/`
2. ✅ Analyze the code
3. ✅ Ask for confirmation
4. ✅ Migrate to `databricks_target/`
5. ✅ Copy README, LICENSE, etc.
6. ✅ Generate detailed reports
7. ✅ Show next steps

---

## After Migration

Your migrated code is in `databricks_target/`:

```bash
# 1. Review the migration report
cat migration_report.txt

# 2. Check a sample migrated file
cat databricks_target/src/readers.py

# 3. Compare before/after
diff glue_source/src/readers.py databricks_target/src/readers.py

# 4. Update requirements.txt
cd databricks_target/
# Remove: awsglue
# Keep: pyspark

# 5. Initialize new Git repo for Databricks code
git init
git add .
git commit -m "Migrated from Glue to Databricks"
git remote add origin <your-databricks-repo-url>
git push -u origin main
```

---

## Folder Structure After Migration

```
/Users/analytics360/glue2lakehouse/
│
├── glue_source/                    ← Your original Glue code
│   ├── src/
│   ├── jobs/
│   └── tests/
│
├── databricks_target/              ← Your migrated Databricks code
│   ├── src/
│   ├── jobs/
│   └── tests/
│
├── analysis_report.txt             ← Pre-migration analysis
├── migration_report.txt            ← Migration results
│
└── glue2lakehouse/                ← Framework code
    ├── core/
    ├── mappings/
    └── ...
```

---

## Real-World Example

```bash
# You have a Glue repo at:
# https://github.com/acme-corp/data-pipeline-glue

cd /Users/analytics360/glue2lakehouse
source venv/bin/activate

# Clone to glue_source/
git clone https://github.com/acme-corp/data-pipeline-glue.git glue_source/

# Migrate to databricks_target/
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog production \
    --verbose \
    --report migration_report.txt

# Results in databricks_target/:
# - All .py files migrated
# - GlueContext → SparkSession
# - DynamicFrame → DataFrame
# - Glue Catalog → Unity Catalog
# - All transforms converted

# Now push to new Databricks repo:
cd databricks_target/
git init
git remote add origin https://github.com/acme-corp/data-pipeline-databricks.git
git add .
git commit -m "Migrated from Glue to Databricks"
git push -u origin main
```

---

## Quick Command Reference

```bash
# Using the migration script (recommended)
./migrate_repo.sh <glue-repo-path-or-url> <catalog-name>

# Manual migration
python -m glue2lakehouse migrate-package \
    -i glue_source/ \
    -o databricks_target/ \
    --catalog <catalog-name>

# With all options
python -m glue2lakehouse migrate-package \
    --input glue_source/ \
    --output databricks_target/ \
    --catalog production \
    --config config.yaml \
    --report migration_report.txt \
    --verbose
```

---

## Troubleshooting

**Q: glue_source/ already exists**
```bash
# Remove it first
rm -rf glue_source/
# Then clone/copy again
```

**Q: databricks_target/ already exists**
```bash
# Remove it to start fresh
rm -rf databricks_target/
# Then run migration again
```

**Q: Want to keep git history**
```bash
# Migrate in a new branch of the same repo
cd glue_source/
git checkout -b databricks-migration
cd ..
python -m glue2lakehouse migrate-package \
    -i glue_source/ \
    -o glue_source_temp/
rm -rf glue_source/*
mv glue_source_temp/* glue_source/
cd glue_source/
git add .
git commit -m "Migrate to Databricks"
```

---

## Summary

**Your workflow:**
1. Copy your Git repo → `glue_source/`
2. Run migration → creates `databricks_target/`
3. Review and test
4. Push to new Databricks repo

**Commands:**
```bash
# Easiest way:
./migrate_repo.sh <your-repo-path> <catalog>

# Or manual:
git clone <repo> glue_source/
python -m glue2lakehouse migrate-package -i glue_source/ -o databricks_target/
```

That's it! 🚀
