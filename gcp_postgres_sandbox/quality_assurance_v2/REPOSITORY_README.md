# CryptoPrism-DB Quality Assurance System v2

🔍 **Advanced PostgreSQL database quality assurance system designed for cryptocurrency data pipelines with intelligent monitoring, automated testing, and real-time alerting.**

[![Health Score](https://img.shields.io/badge/Health%20Score-100%2F100-brightgreen)](./QA_STATUS_SUMMARY.md)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue.svg)](https://postgresql.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🚀 Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-username/cryptoprism-qa-system.git
cd cryptoprism-qa-system

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your database credentials

# 4. Run QA tests
python run_qa.py --individual
```

## ⚡ Features

### **🔬 Individual QA Tests** (Recommended for Development)
- **Modular Design** - Each test runs independently for focused debugging
- **Smart Notifications** - Individual Telegram alerts + combined summary
- **AI Analysis** - OpenAI-powered issue summaries for critical problems
- **Health Scoring** - Visual health indicators (✅🟡🟠🔴)

### **🏢 Comprehensive Analysis** (Production Monitoring)
- **Full Database Sweep** - Complete analysis across all monitored tables
- **Business Logic Validation** - CryptoPrism-specific rules and requirements
- **Historical Trending** - Health score tracking over time
- **Executive Reporting** - Automated report generation

### **🤖 Intelligent Automation**
- **AI-Powered Issue Detection** - GPT/Gemini integration for smart analysis
- **Risk-Based Alerting** - Prioritized notifications based on severity
- **Automated Remediation** - Self-healing capabilities for common issues
- **Trend Analysis** - Performance regression detection

## 📁 Project Structure

```
cryptoprism-qa-system/
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
│   │   ├── test_data_quality.py        # NULL analysis & integrity
│   │   ├── test_timestamp_validation.py # Timestamp rules & freshness
│   │   ├── test_duplicate_detection.py # Duplicate checking
│   │   └── test_business_logic.py      # Business validation rules
│   ├── run_individual_tests.py # Individual test runner
│   └── run_comprehensive_qa.py # Comprehensive analysis
├── 📂 utils/                   # Utility scripts
│   ├── direct_db_check.py      # Direct database verification
│   └── verify_database_schema.py # Schema validation
├── 📂 _archive/                # Legacy/deprecated files
├── 📂 logs/                    # QA execution logs (auto-created)
├── 📂 reports/                 # Generated QA reports (auto-created)
├── run_qa.py                   # 🎯 MAIN ENTRY POINT
├── requirements.txt            # Python dependencies
├── .env.example               # Environment configuration template
└── README.md                  # This file
```

## 🎯 Usage Examples

### **Individual Tests** (Recommended for Development)
```bash
# Run all individual tests with separate notifications
python run_qa.py --individual

# Run specific test for focused debugging
python run_qa.py --test data_quality      # NULL analysis
python run_qa.py --test timestamps        # Timestamp validation  
python run_qa.py --test duplicates        # Duplicate detection
python run_qa.py --test business_logic    # Business rules

# Direct execution for debugging
python tests/individual/test_data_quality.py
```

### **Comprehensive Analysis** (Production Monitoring)
```bash
# Full comprehensive QA analysis
python run_qa.py --comprehensive

# List all available tests and options
python run_qa.py --list
```

### **Schema-Aligned Quick Test** (Current Working Version)
```bash
# Run the proven working test with actual database schema
python quick_qa_test.py
```

## 🔧 Configuration

### **Environment Setup**
1. **Copy configuration template**:
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your settings**:
   ```env
   # Database (Required)
   DB_HOST=your_postgresql_host
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_PORT=5432
   DB_NAME=dbcp
   
   # Notifications (Optional)
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   OPENAI_API_KEY=your_openai_key
   ```

### **Database Requirements**
- **PostgreSQL 12+** with the following tables:
  - `FE_DMV_ALL` - DMV aggregated signals
  - `FE_DMV_SCORES` - Durability/Momentum/Valuation scores
  - `1K_coins_ohlcv` - OHLCV historical data
  - `crypto_listings_latest_1000` - Market listings

## 📊 Test Coverage

| Test Module | Focus Area | Critical Checks |
|-------------|------------|----------------|
| **Data Quality** | NULL analysis, integrity | ✅ >50% NULL = CRITICAL |
| **Timestamps** | Business rules, freshness | ✅ FE table consistency |
| **Duplicates** | Key uniqueness | ✅ (slug, timestamp) pairs |
| **Business Logic** | Domain rules | ✅ DMV scores, market caps |

## 📱 Telegram Notifications

Each test sends focused alerts with health scoring:

```
🔬 Data Quality Analysis ✅
Health Score: 98.5/100
Time: 14:23 UTC

📋 Test Results
• Total Checks: 24
• Passed: 22 | Failed: 0  
• Warnings: 2 | Errors: 0

⚠️ Risk Levels
🔴 Critical: 0 | 🟠 High: 0
🟡 Medium: 2 | 🟢 Low: 22

✅ Status: GOOD - Minor issues
```

## 🚀 Installation & Setup

### **Prerequisites**
- Python 3.8+
- PostgreSQL 12+
- Access to CryptoPrism-DB database

### **Installation Steps**
1. **Clone and setup**:
   ```bash
   git clone https://github.com/your-username/cryptoprism-qa-system.git
   cd cryptoprism-qa-system
   pip install -r requirements.txt
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Test connection**:
   ```bash
   python quick_qa_test.py
   ```

4. **Run full QA suite**:
   ```bash
   python run_qa.py --individual
   ```

## 🔄 CI/CD Integration

### **GitHub Actions Example**
```yaml
name: Database QA
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:

jobs:
  qa-check:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Setup Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: pip install -r requirements.txt
      
    - name: Run QA Tests
      run: python run_qa.py --individual
      env:
        DB_HOST: ${{ secrets.DB_HOST }}
        DB_USER: ${{ secrets.DB_USER }}
        DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
        TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
        TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
```

## 📈 Health Score Interpretation

| Score Range | Status | Description |
|-------------|--------|-------------|
| 95-100 | ✅ **EXCELLENT** | All systems optimal |
| 80-94 | 🟡 **GOOD** | Minor issues detected |
| 60-79 | 🟠 **ISSUES** | Problems require attention |
| 0-59 | 🔴 **CRITICAL** | Immediate action needed |

## 🤝 Contributing

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/new-qa-test`
3. **Add your test**: Follow existing patterns in `tests/individual/`
4. **Update documentation**: Add test description to README
5. **Submit pull request**: Include test results and rationale

## 📚 Documentation

- **[QA Status Summary](./QA_STATUS_SUMMARY.md)** - Current system health
- **[QA Report Log](./QA_REPORT_LOG.md)** - Historical execution records  
- **[Architecture Overview](./README.md)** - System design and components

## 🆘 Troubleshooting

### **Common Issues**
1. **Import Errors**: Ensure you're running from the repository root
2. **Database Connection**: Verify credentials in `.env` file
3. **Schema Mismatches**: Use `quick_qa_test.py` for schema-aligned testing
4. **Telegram Notifications**: Check bot token and chat ID configuration

### **Getting Help**
- **Check logs**: `./logs/` directory for execution details
- **Review reports**: `./reports/` directory for detailed analysis
- **Run diagnostics**: `python utils/direct_db_check.py`

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🎯 Roadmap

- [ ] **Web Dashboard** - Real-time QA status visualization
- [ ] **Advanced Analytics** - Machine learning anomaly detection  
- [ ] **Multi-Database** - Support for additional database types
- [ ] **Performance Benchmarking** - Query optimization recommendations
- [ ] **Auto-Remediation** - Self-healing database maintenance

---

**Built for CryptoPrism-DB** | **Ensuring Database Excellence** | **v2.1.0**