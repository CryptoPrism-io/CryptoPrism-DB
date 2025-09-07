"""
CryptoPrism-DB Quality Assurance v2 - Core Module
Enhanced database quality assurance system for cryptocurrency data pipelines.
"""

__version__ = "2.0.0"
__author__ = "CryptoPrism-DB QA Team"

from .config import QAConfig
from .database import DatabaseManager
from .base_qa import BaseQAModule

__all__ = ['QAConfig', 'DatabaseManager', 'BaseQAModule']