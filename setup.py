# setup.py
"""
Setup script for NYC Taxi Data Ingestion Pipeline
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

# Read requirements
requirements = []
if (this_directory / "requirements.txt").exists():
    with open("requirements.txt", "r") as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="nyc-taxi-ingestion",
    version="1.0.0",
    author="Data Engineering Team",
    author_email="data-eng@company.com",
    description="Production-ready NYC Taxi data ingestion pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/nyc-taxi-ingestion",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Database",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0", 
            "pytest-mock>=3.10.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "docs": [
            "sphinx>=6.0.0",
            "sphinx-rtd-theme>=1.2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "nyc-taxi-ingest=scripts.run_ingestion:main",
        ],
    },
    package_data={
        "": ["*.txt", "*.md", "*.yml", "*.yaml"],
    },
    include_package_data=True,
    zip_safe=False,
)