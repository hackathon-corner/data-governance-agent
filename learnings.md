# Learnings from Building the Data Governance Pipeline & Agentic AI System

This document summarizes the challenges, debugging paths, architectural decisions, and key lessons learned during the development of the **Data Governance Pipeline**, **Coordinator Agent**, **Auto-run Agent**, and the **Streamlit Dashboard UI**.

It serves as a long-term reference for future improvements and as a reflection of what we learned technically and conceptually.

## 1. Understanding the System Architecture

While building the project, we gained a deeper understanding of how the system components interact:

1. Raw CSV inputs (events, users, courses)  
2. Schema validation  
3. Data quality checks  
4. PII policy enforcement  
5. Foreign key integrity checks  
6. Coordinator agent orchestrating all validations  
7. Automatic Markdown + JSON summarization  
8. Streamlit Dashboard UI for human-readable inspection

---

## 2. Issue: UI Showing Old Pipeline Results

### Symptom
- Running the pipeline via CLI produced correct results.
- The Streamlit dashboard sometimes displayed outdated PASS/FAIL status.

### Root Cause
Several modules contained top-level global initializations like:

```python
config = load_config()
```

Because Streamlit caches imported modules, these values never refreshed.

### Fix
All top-level config loads were removed, and replaced with:

```python
def run_pipeline(...):
    config = load_config()
```

### Lesson
**Never load configuration or runtime state at module import time.**

---

## 3. Issue: `KeyError: 'summary'` in UI

### Symptom
UI crashed with:

```
KeyError: 'summary'
```

### Root Cause
`generate_markdown_report()` returned inconsistent structures.

### Fix
Standardized all return values to include:

```python
{
    "summary": {...},
    "report_path": "reports/..."
}
```

### Lesson
**UIs must rely on predictable return formats.**

---

## 4. Issue: Test Data Not Found in Pytests

### Symptom
Tests failed with FileNotFoundError for CSV files.

### Root Cause
Renamed datasets but tests still referenced old paths.

### Fix
Updated all test paths and added comprehensive good/bad datasets.

---

## 5. Issue: JSON Serialization Errors

### Symptom
Error when printing JSON:

```
TypeError: Object of type ... is not JSON serializable
```

### Root Cause
Summary dictionaries contained non-JSON-safe objects (pathlib, pandas types).

### Fix
Sanitized summary before serialization.

---

## 6. Understanding `load_config()` Use

### Key Learning
Some modules called `load_config()` at import time, causing stale behavior.

### Lesson
**Config should be loaded inside runtime functions only.**

---

## 7. Streamlit State & Module Caching

### Takeaways
- Streamlit re-executes the script, not imports.
- Imported modules stay cached.
- Global variables remain stale.

### Lesson
**Data loading must happen inside event callbacks or runtime functions.**

---

## 8. Markdown Reports & Run IDs

Each pipeline run generates:
- curated CSV file  
- markdown governance report  
- JSON summary  

This ensures reproducibility and auditability.

---

## 9. Agent Integration Learnings

Modular check functions made agent orchestration clean and testable.

---

## 10. Data Quality Insights

Iterating through test cases taught:
- null thresholds  
- unique keys  
- allowed values  
- referential integrity  

---

## 11. Future Improvements

- Spark support  
- Delta Lake  
- Trend visualizations  
- Cloud Run deployment  
- Pub/Sub automation  
- Notifications  

---

## Final Reflection

**This project reinforced deep lessons on data governance, pipeline design, agent orchestration, and UI-backend synchronization.**
