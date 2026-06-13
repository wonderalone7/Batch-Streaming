# PySpark ETL Pipeline Project

## Environment Setup ✅

Your PySpark development environment is ready!

### Installed Packages
- **PySpark 3.5.0** - Distributed computing framework
- **Pandas 2.1.4** - Data manipulation
- **NumPy 1.26.3** - Numerical computing
- **Pytest 7.4.3** - Testing framework
- **Kafka-Python 2.0.2** - Kafka integration
- **Apache-Airflow 2.7.3** - Workflow orchestration
- **Requests 2.31.0** - HTTP library
- **Python-dotenv 1.0.0** - Environment variable management

### Environment Variables
- **JAVA_HOME**: C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot
- **Python Version**: 3.11.5
- **Virtual Environment**: `.venv/`

### Project Structure
```
PySpark/
├── src/                    # Your source code
├── tests/                  # Test files
├── data/                   # Input/output data
├── config/                 # Configuration files
├── logs/                   # Application logs
├── requirements.txt        # Package list
├── .env                    # Environment variables
├── .gitignore              # Git ignore rules
└── .vscode/                # VS Code settings
    ├── settings.json       # Interpreter & formatting
    └── launch.json         # Debug configurations
```

## Quick Start

1. **Verify the environment:**
   ```bash
   .venv\Scripts\python.exe -c "import pyspark; print(pyspark.__version__)"
   ```

2. **Create your first script in `src/`**
   - VS Code will auto-detect the Python interpreter
   - Debug using F5 or the Run & Debug panel

3. **Run tests:**
   ```bash
   .venv\Scripts\pytest tests/
   ```

4. **Load environment variables:**
   ```python
   from dotenv import load_dotenv
   load_dotenv()
   ```

## VS Code Configuration

- **Python Interpreter**: Automatically configured to `.venv`
- **Debugging**: Two launch configs available
  - "Python: Current File" - Standard debug
  - "Python: PySpark Debug" - With full PYTHONPATH
- **Testing**: Pytest configured, auto-discovery in `tests/`
- **Formatting**: Black formatter enabled

## Notes

- All Spark/Java environment variables are pre-configured
- The `.venv` folder is excluded from git (see `.gitignore`)
- Use `python -m pip install -r requirements.txt` to sync dependencies
- Start coding in `src/` directory

Happy coding! 🚀
