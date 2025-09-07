# Setup Instructions - CryptoPrism-DB QA System v2

This guide will help you set up the QA system as a standalone repository.

## 🚀 Quick Setup for New Repository

### **Step 1: Create New Repository**
```bash
# Create new repository on GitHub/GitLab
# Then clone locally
git clone https://github.com/your-username/cryptoprism-qa-system.git
cd cryptoprism-qa-system
```

### **Step 2: Copy QA System Files**
```bash
# Copy the entire quality_assurance_v2 directory to your new repository
# Make sure to include all subdirectories and files:
#   - core/
#   - reporting/  
#   - tests/
#   - utils/
#   - _archive/
#   - All .py files
#   - requirements.txt
#   - .env.example
#   - .gitignore
#   - README files
```

### **Step 3: Initialize Git Repository**
```bash
# Initialize git in the new directory
git init
git add .
git commit -m "🎯 Initial commit: CryptoPrism-DB QA System v2

- Complete modular QA architecture
- Individual test modules for focused debugging  
- Schema-aligned tests with 100/100 health score
- Telegram notifications and AI integration
- Comprehensive documentation and examples"

# Add remote and push
git remote add origin https://github.com/your-username/cryptoprism-qa-system.git
git branch -M main
git push -u origin main
```

## 🔧 Environment Configuration

### **Step 4: Setup Python Environment**
```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### **Step 5: Configure Database Connection**
```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your actual credentials
nano .env  # or use any text editor
```

**Required `.env` configuration**:
```env
# Database (Required)
DB_HOST=your_postgresql_host
DB_USER=your_database_username
DB_PASSWORD=your_database_password
DB_PORT=5432
DB_NAME=dbcp

# Notifications (Optional)
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
OPENAI_API_KEY=your_openai_api_key
```

## 🧪 Testing Setup

### **Step 6: Verify Installation**
```bash
# Test database connection
python quick_qa_test.py

# Should output:
# =================================
# === CryptoPrism-DB Quick QA Test ===
# Health Score: 100.0/100
# Overall Status: EXCELLENT
# =================================
```

### **Step 7: Run Full QA Suite**
```bash
# Run all individual tests
python run_qa.py --individual

# Run specific test
python run_qa.py --test data_quality

# List all available options
python run_qa.py --list
```

## 📁 Directory Structure Verification

After setup, your repository should look like this:

```
cryptoprism-qa-system/
├── .env                        # Your database credentials
├── .env.example               # Template for others
├── .gitignore                 # Git ignore rules
├── requirements.txt           # Python dependencies
├── README.md                  # Main documentation
├── SETUP.md                  # This file
├── run_qa.py                 # Main entry point
├── quick_qa_test.py          # Working schema-aligned test
├── QA_REPORT_LOG.md          # Historical execution records
├── QA_STATUS_SUMMARY.md      # Current health dashboard
├── core/                     # Core components
│   ├── __init__.py
│   ├── base_qa.py
│   ├── config.py
│   └── database.py
├── reporting/                # Notifications & reports
│   ├── __init__.py
│   ├── notification_system.py
│   ├── qa_report_logger.py
│   └── report_generator.py
├── tests/                    # Test modules
│   ├── __init__.py
│   ├── run_individual_tests.py
│   ├── run_comprehensive_qa.py
│   └── individual/           # Individual test files
│       ├── __init__.py
│       ├── test_data_quality.py
│       ├── test_timestamp_validation.py
│       ├── test_duplicate_detection.py
│       └── test_business_logic.py
├── utils/                    # Utility scripts
│   ├── __init__.py
│   ├── direct_db_check.py
│   └── verify_database_schema.py
├── _archive/                 # Legacy files (optional)
├── logs/                     # Auto-created for log files
└── reports/                  # Auto-created for reports
```

## 🔧 Advanced Configuration

### **Telegram Notifications Setup**
1. **Create Telegram Bot**:
   ```bash
   # Message @BotFather on Telegram
   # Send: /newbot
   # Follow instructions to get BOT_TOKEN
   ```

2. **Get Chat ID**:
   ```bash
   # Add bot to your chat/channel
   # Send a message, then visit:
   # https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates
   # Look for "chat":{"id": YOUR_CHAT_ID}
   ```

### **OpenAI Integration Setup**
1. **Get API Key**:
   - Visit: https://platform.openai.com/api-keys
   - Create new secret key
   - Add to `.env` as `OPENAI_API_KEY=your_key_here`

### **GitHub Actions Setup**
1. **Add Secrets** in your GitHub repository:
   - `DB_HOST`
   - `DB_USER`
   - `DB_PASSWORD`
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - `OPENAI_API_KEY`

2. **Create workflow file** `.github/workflows/qa-check.yml`:
   ```yaml
   name: Daily QA Check
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
           OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
   ```

## ✅ Verification Checklist

After setup, verify everything works:

- [ ] **Repository cloned and initialized**
- [ ] **Dependencies installed** (`pip install -r requirements.txt`)
- [ ] **Environment configured** (`.env` file with database credentials)
- [ ] **Database connection working** (`python quick_qa_test.py` shows 100/100 health)
- [ ] **Individual tests working** (`python run_qa.py --individual`)
- [ ] **Notifications configured** (Telegram/OpenAI optional but recommended)
- [ ] **Git repository setup** (committed and pushed to remote)
- [ ] **Documentation accessible** (README.md, QA reports)

## 🆘 Troubleshooting

### **Common Setup Issues**

1. **Import Errors**:
   ```bash
   # Make sure you're in the repository root directory
   pwd  # Should show: /path/to/cryptoprism-qa-system
   python run_qa.py --list  # Should work without errors
   ```

2. **Database Connection Issues**:
   ```bash
   # Test connection directly
   python -c "
   from core.config import QAConfig
   from core.database import DatabaseManager
   config = QAConfig()
   db = DatabaseManager(config)
   print('Connection:', db.test_connection('dbcp'))
   "
   ```

3. **Missing Dependencies**:
   ```bash
   # Reinstall all dependencies
   pip install --force-reinstall -r requirements.txt
   ```

4. **Permission Issues**:
   ```bash
   # Make sure run_qa.py is executable
   chmod +x run_qa.py
   ```

## 🎯 Next Steps

1. **Test the system** with your database
2. **Customize thresholds** in `.env` for your specific needs  
3. **Set up automated monitoring** with GitHub Actions
4. **Configure notifications** for your team
5. **Integrate with your CI/CD pipeline**

## 📞 Support

If you encounter issues during setup:

1. **Check logs**: `./logs/` directory
2. **Review configuration**: Verify `.env` file  
3. **Test components**: Use individual test files for debugging
4. **Check documentation**: QA_STATUS_SUMMARY.md and QA_REPORT_LOG.md

---

**Setup Complete!** 🎉 Your CryptoPrism-DB QA System v2 is ready to monitor database health and ensure data integrity.