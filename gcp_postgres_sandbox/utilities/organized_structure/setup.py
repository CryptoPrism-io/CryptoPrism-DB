#!/usr/bin/env python3
"""
Setup configuration for CryptoPrism Database Utilities.

A comprehensive suite of PostgreSQL database analysis, benchmarking, and optimization tools
specifically designed for cryptocurrency trading systems.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file for long description
readme_path = Path(__file__).parent / "README.md"
if readme_path.exists():
    with open(readme_path, "r", encoding="utf-8") as f:
        long_description = f.read()
else:
    long_description = __doc__

# Read version from package __init__.py
def get_version():
    """Extract version from package __init__.py"""
    init_path = Path(__file__).parent / "src" / "crypto_db_utils" / "__init__.py"
    with open(init_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"').strip("'")
    return "1.0.0"

setup(
    name="cryptoprism-db-utils",
    version=get_version(),
    author="CryptoPrism.io",
    author_email="dev@cryptoprism.io",
    description="PostgreSQL database utilities for cryptocurrency analysis systems",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CryptoPrism-io/CryptoPrism-DB-Utils",
    project_urls={
        "Bug Tracker": "https://github.com/CryptoPrism-io/CryptoPrism-DB-Utils/issues",
        "Documentation": "https://github.com/CryptoPrism-io/CryptoPrism-DB-Utils/blob/main/README.md",
        "Source": "https://github.com/CryptoPrism-io/CryptoPrism-DB-Utils",
    },
    
    # Package configuration
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    
    # Python version requirement
    python_requires=">=3.8",
    
    # Dependencies
    install_requires=[
        "python-dotenv>=1.0.1",
        "pandas>=2.2.2", 
        "numpy>=1.26.4",
        "sqlalchemy>=2.0.32",
        "psycopg2-binary>=2.9.0",
        "requests>=2.32.3",
        "typing-extensions>=4.12.2",
    ],
    
    # Optional dependencies for specific features
    extras_require={
        "visualization": [
            "matplotlib>=3.7.0",
            "seaborn>=0.12.0", 
            "sqlalchemy-schemadisplay>=1.3",
            "graphviz>=0.20.0",
        ],
        "ai": [
            "google-generativeai>=0.5.0",
        ],
        "notifications": [
            "python-telegram-bot>=13.15",
        ],
        "mysql": [
            "mysql-connector-python>=9.0.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "pre-commit>=3.0.0",
        ],
        "all": [
            "matplotlib>=3.7.0",
            "seaborn>=0.12.0",
            "sqlalchemy-schemadisplay>=1.3", 
            "graphviz>=0.20.0",
            "google-generativeai>=0.5.0",
            "python-telegram-bot>=13.15",
            "mysql-connector-python>=9.0.0",
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "pre-commit>=3.0.0",
        ]
    },
    
    # Console scripts for CLI access
    entry_points={
        "console_scripts": [
            "cryptoprism-db=cli.main:main",
            "cpdb=cli.main:main",  # Short alias
        ],
    },
    
    # Classification
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9", 
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Environment :: Console",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    
    # Keywords for PyPI search
    keywords=[
        "postgresql", "database", "optimization", "cryptocurrency", "trading",
        "analysis", "benchmarking", "performance", "indexing", "schema"
    ],
    
    # Package data
    package_data={
        "crypto_db_utils": [
            "py.typed",  # For type checking support
        ],
    },
    
    # Zip safe
    zip_safe=False,
)