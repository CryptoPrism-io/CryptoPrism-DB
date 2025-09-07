# CryptoPrism-DB Quality Assurance System v2

🔍 **Advanced database quality assurance system for cryptocurrency data pipelines with intelligent monitoring, data integrity validation, and automated alerting.**

## 🚀 Quick Start

```bash
# Run all QA tests (recommended)
python run_qa.py --individual

# Run specific test for debugging
python run_qa.py --test data_quality

# See all available options
python run_qa.py --help
```

## 📁 Project Structure

```
quality_assurance_v2/
├── 📂 core/                    # Core system components
│   ├── base_qa.py              # QA result data classes
│   ├── config.py               # Configuration management
│   └── database.py             # Database connection handling
├── 📂 reporting/               # Notification and reporting
│   ├── notification_system.py  # Telegram + AI notifications
│   ├── qa_report_logger.py     # Historical QA tracking
│   └── report_generator.py     # Report formatting
├── 📂 tests/                   # QA test modules
│   ├── individual/             # Individual test files
│   │   ├── test_data_quality.py        # NULL analysis
│   │   ├── test_timestamp_validation.py # Timestamp rules
│   │   ├── test_duplicate_detection.py # Duplicate checking
│   │   └── test_business_logic.py      # Business validation
│   ├── run_individual_tests.py # Individual test runner
│   └── run_comprehensive_qa.py # Comprehensive analysis
├── 📂 utils/                   # Utility scripts
│   ├── direct_db_check.py      # Direct database verification
│   └── verify_database_schema.py # Schema validation
├── 📂 _archive/                # Legacy/deprecated files
├── 📂 logs/                    # QA execution logs
├── 📂 reports/                 # Generated QA reports
├── run_qa.py                   # 🎯 MAIN ENTRY POINT
└── README.md                   # This file
```

## ⚡ Features

### **Individual QA Tests** (Recommended)
- 🔬 **Modular Design** - Each test runs independently for easy debugging
- 🔔 **Smart Notifications** - Individual Telegram alerts + combined summary
- 🤖 **AI Analysis** - OpenAI-powered issue summaries for critical problems
- 📊 **Health Scoring** - Clear visual health indicators (✅🟡🟠🔴)

### **Core Capabilities**
- ✅ **Data Quality Analysis** - NULL percentage thresholds and data integrity
- ✅ **Timestamp Validation** - Business logic rules and freshness checks
- ✅ **Duplicate Detection** - Composite key (slug, timestamp) validation
- ✅ **Business Logic** - CryptoPrism-specific validation rules
- ✅ **Historical Tracking** - QA execution history and trend analysis

## 🎯 Usage Examples

### Run Individual Tests (Recommended)
```bash
# All individual tests with separate notifications
python run_qa.py --individual

# Specific test for focused debugging
python run_qa.py --test data_quality
python run_qa.py --test timestamps
python run_qa.py --test duplicates
python run_qa.py --test business_logic
```

### Run Individual Test Files Directly
```bash
# Direct execution for debugging
python tests/individual/test_data_quality.py
python tests/individual/test_timestamp_validation.py
python tests/individual/test_duplicate_detection.py
python tests/individual/test_business_logic.py
```

### Comprehensive Analysis
```bash
# Full comprehensive QA (includes original prod tests)
python run_qa.py --comprehensive
```

## 📱 Telegram Notifications

Each test sends focused alerts:

```
🔬 *Data Quality Analysis* ✅
Health Score: *98.5/100*
Time: 14:23 UTC

📋 *Test Results*
• Total Checks: 24
• Passed: 22 | Failed: 0
• Warnings: 2 | Errors: 0

⚠️ *Risk Levels*
🔴 Critical: 0 | 🟠 High: 0
🟡 Medium: 2 | 🟢 Low: 22

✅ *Status: GOOD* - Minor issues
```

## 🔧 Configuration

### Environment Variables
```env
# Database (Required)
DB_HOST=your_postgresql_host
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432

# Notifications (Optional)
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
OPENAI_API_KEY=your_openai_api_key
```

### QA Thresholds (Optional)
```env
QA_NULL_RATIO_LOW=0.05      # 5% NULL threshold
QA_NULL_RATIO_MEDIUM=0.20   # 20% NULL threshold  
QA_NULL_RATIO_HIGH=0.50     # 50% NULL threshold
QA_MIN_TIMESTAMP_RANGE_DAYS=4
```

## 🎯 Key Benefits

### **For Debugging:**
- ✅ **Individual Tests** - Run specific tests without full suite
- ✅ **Clear Error Messages** - Focused error reporting per test
- ✅ **Modular Failures** - One test failure doesn't stop others

### **For Production Monitoring:**
- ✅ **Intelligent Alerts** - AI-powered issue prioritization
- ✅ **Health Scoring** - Quick visual status assessment
- ✅ **Historical Tracking** - Trend analysis over time

### **For DevOps:**
- ✅ **Multiple Entry Points** - CLI args, direct execution, imports
- ✅ **Flexible Notification** - Telegram, logs, reports
- ✅ **CI/CD Ready** - Environment-aware configuration

## 📊 Test Coverage

| Test Module | Coverage | Critical Checks |
|-------------|----------|----------------|
| **Data Quality** | NULL analysis, data integrity | ✅ >50% NULL = CRITICAL |
| **Timestamps** | Business rules, freshness | ✅ FE table consistency |
| **Duplicates** | Composite key validation | ✅ (slug, timestamp) uniqueness |
| **Business Logic** | CryptoPrism rules | ✅ DMV scores, market cap validation |

## 🚀 Integration

### Import in Python Scripts
```python
from quality_assurance_v2.tests.individual.test_data_quality import test_data_quality
from quality_assurance_v2.core.config import QAConfig
from quality_assurance_v2.reporting.notification_system import NotificationSystem

# Run specific test
results = test_data_quality()

# Send notification
config = QAConfig()
notifier = NotificationSystem(config)
notifier.send_individual_test_alert("Data Quality", results, "data_quality")
```

### GitHub Actions Integration
```yaml
- name: Run QA Tests
  run: python gcp_postgres_sandbox/quality_assurance_v2/run_qa.py --individual
  env:
    DB_HOST: ${{ secrets.DB_HOST }}
    DB_USER: ${{ secrets.DB_USER }}
    DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
    TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
    TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
    OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
```

---

## 🔄 Migration from v1

**Old approach:** Single comprehensive test with mixed results
```bash
python prod_qa_dbcp.py  # Old v1 approach
```

**New approach:** Modular individual tests for better debugging
```bash
python run_qa.py --individual  # New v2 approach
```

**Key improvements:**
- 🎯 **Focused debugging** - Run specific tests
- 📱 **Better notifications** - Individual + summary alerts  
- 🤖 **AI insights** - Smart issue prioritization
- 📊 **Health scoring** - Visual status indicators
- 🔄 **Historical tracking** - Trend analysis over time

---

*Quality Assurance v2 - Reorganized for logical structure and focused debugging*