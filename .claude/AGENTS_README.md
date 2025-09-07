# Claude Code Custom Agents

This directory contains custom Claude Code agents for the CryptoPrism-DB project. Agents are specialized AI assistants that can be invoked for specific tasks using the Task tool.

## Available Agents

### 1. Database Analyst (`database-analyst`)
- **Purpose**: PostgreSQL database operations, analysis, and optimization
- **Use Cases**: Schema analysis, query optimization, database migrations, GCP PostgreSQL operations
- **Tools**: Read, Write, Edit, Bash, Grep, Glob

### 2. Crypto Data Engineer (`crypto-data-engineer`)
- **Purpose**: Cryptocurrency data pipelines, ETL processes, and blockchain data analysis
- **Use Cases**: Data ingestion, API integration, time-series optimization, data validation
- **Tools**: Read, Write, Edit, Bash, Grep, Glob, WebFetch

### 3. Python Developer (`python-developer`)
- **Purpose**: Python development for data analysis, web scraping, and automation
- **Use Cases**: Clean Python code, data processing pipelines, API development, testing
- **Tools**: Read, Write, Edit, Bash, Grep, Glob

### 4. DevOps Engineer (`devops-engineer`)
- **Purpose**: CI/CD pipelines, deployment automation, and cloud operations
- **Use Cases**: GitHub Actions, Docker, cloud infrastructure, monitoring, rollback procedures
- **Tools**: Read, Write, Edit, Bash, Grep, Glob

### 5. Security Analyst (`security-analyst`)
- **Purpose**: Code security review, vulnerability assessment, and security best practices
- **Use Cases**: Security code reviews, vulnerability remediation, secure authentication
- **Tools**: Read, Write, Edit, Bash, Grep, Glob
- **Note**: Focused on defensive security only

### 6. Data Scientist (`data-scientist`)
- **Purpose**: Cryptocurrency market analysis, statistical modeling, and machine learning
- **Use Cases**: Market analysis, predictive models, statistical reports, ML pipelines
- **Tools**: Read, Write, Edit, Bash, Grep, Glob

### 7. Documentation Specialist (`documentation-specialist`)
- **Purpose**: Technical documentation creation and maintenance
- **Use Cases**: README files, API docs, user guides, architecture documentation
- **Tools**: Read, Write, Edit, Bash, Grep, Glob

### 8. Code Reviewer (`code-reviewer`)
- **Purpose**: Code quality assurance, best practices, and maintainability
- **Use Cases**: Code reviews, quality assessment, performance optimization, security checks
- **Tools**: Read, Write, Edit, Bash, Grep, Glob

## Usage

To use a custom agent, use the Task tool with the agent's name:

```
Task(subagent_type="database-analyst", description="Analyze database schema", prompt="Please analyze the current database schema and suggest optimizations")
```

## Agent File Format

Agents are defined as Markdown files with YAML frontmatter:

```markdown
---
name: agent-name
description: Brief description of when this agent should be used
tools: tool1, tool2, tool3  # Optional - inherits all tools if omitted
---

Agent system prompt and instructions go here...
```

## Directory Structure

```
.claude/
├── settings.local.json
├── agents/
│   ├── database-analyst.md
│   ├── crypto-data-engineer.md
│   ├── python-developer.md
│   ├── devops-engineer.md
│   ├── security-analyst.md
│   ├── data-scientist.md
│   ├── documentation-specialist.md
│   └── code-reviewer.md
└── AGENTS_README.md (this file)
```

## Notes

- Agents are automatically discovered by Claude Code from this directory
- Each agent is specialized for specific project domains and use cases
- Agents inherit project permissions and settings
- Agent discovery may require restarting Claude Code