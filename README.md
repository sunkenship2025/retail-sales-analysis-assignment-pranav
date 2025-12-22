

---

```markdown
# Python Intern Assignment

## Overview

This project demonstrates a modular, testable data pipeline for retail sales analysis using Python, SQLite, and pandas.

---

## Key Features

- **PEP 8 Compliance:**  
  All code is formatted according to [PEP 8](https://peps.python.org/pep-0008/) standards for readability and consistency.

- **Logging:**  
  Logging is set up using Python’s `logging` module. All major operations (especially in data loading) are logged to `loading.log` for traceability.

- **Error Handling:**  
  Functions use `try`/`except` blocks to catch and log errors, returning safe defaults (like empty DataFrames) when failures occur.

- **Modularization:**  
  The original Jupyter notebook code was refactored into separate, reusable Python modules:
  - `data_loading.py` for database operations
  - `processing.py` for data transformations
  - `analysis.py` for metrics and reporting
  - `utils.py` for shared utilities (e.g., logging setup)

---

## Testing

### Unit Testing

- Each module has its own unit tests (in `tests/`), covering individual functions with minimal data.
- Run all unit tests with:
  ```sh
  pytest
  ```

### Integration Testing

- Integration tests check that multiple modules work together as a pipeline.
- Example:  
  - Load data from a temporary SQLite DB  
  - Process and merge  
  - Run analysis functions  
  - Assert that outputs match expectations
- See `tests/test_integration.py` for full-pipeline and boundary integration tests.

---

## How to Run

1. **Install dependencies:**
   ```sh
   pip install pandas pytest
   ```

2. **Run all tests:**
   ```sh
   pytest
   ```

---

## Project Structure

```
project_completed/
│
├── data_loading.py
├── processing.py
├── analysis.py
├── utils.py
│
├── tests/
│   ├── test_loading.py
│   ├── test_processing.py
│   ├── test_analysis.py
│   └── test_integration.py
```

---

## Notes

- The code is modular and easy to extend.
- Logging and error handling make debugging and maintenance easier.
- Both unit and integration tests ensure reliability and correctness.

```
