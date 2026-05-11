# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **dbt project** for the Municipal Health Department of Rio de Janeiro (SMS/RJ), modeling health data in BigQuery. The project integrates data from multiple health systems (electronic health records, administrative systems, public health databases) and uses Recce for environment comparison.

**Tech Stack**: dbt-core 1.8.4, dbt-bigquery 1.8.2, Python 3.10.x, Poetry, BigQuery, Recce

## Environment Setup

### Initial Setup
```bash
# Create and activate virtual environment
poetry shell

# Install dependencies (no root package)
poetry install --no-root

# Install dbt packages (dbt-utils, etc.)
dbt deps
```

### Required Environment Variables
- `DBT_USER`: Your name/identifier (used for dev schema naming)
- `DBT_PROFILES_DIR`: Path to directory containing `profiles.yml` with BigQuery credentials

### BigQuery Profiles
The project uses 4 environments defined in `profiles.yml`:
- **dev** (rj-sms-dev): Development environment, default target
- **prod** (rj-sms): Production environment
- **ci** (rj-sms-dev): CI/CD environment
- **sandbox** (rj-sms-sandbox): Sandbox environment

All profiles use service account authentication with keyfile at `/tmp/credentials.json`.

## Common Development Commands

### dbt Workflow
```bash
# Run all models
dbt run

# Run specific model
dbt run -s "nome_modelo"

# Run models with specific tag
dbt run -s tag:daily

# Run models and downstream dependencies
dbt run -s "nome_modelo+"

# Run models and upstream dependencies  
dbt run -s "+nome_modelo"

# Run tests
dbt test

# Run tests for specific model
dbt test -s "nome_modelo"

# Run tests with specific tag
dbt test -s tag:sua_tag

# Build (run + test) models
dbt build

# Build specific subset
dbt build -s staging.*

# Generate and serve documentation
dbt docs generate && dbt docs serve
# Opens at localhost:8080
```

### Recce Workflow (Environment Comparison)
Recce compares data between production and your branch before merge:
```bash
# Compare current branch against prod
./tools/recce.sh

# Compare specific branch
./tools/recce.sh --branch feature-branch

# Use different target environment
./tools/recce.sh --target staging

# Dry run (shows what would be executed)
./tools/recce.sh --dry-run

# Combined options
./tools/recce.sh -b feature-branch -t staging
```
Opens UI at `http://localhost:8000`. See `tools/recce.md` for detailed documentation.

### Code Quality
```bash
# Run all linters (black, isort, flake8)
task lint

# Pre-commit hooks
pre-commit run --all-files
```

## Project Architecture

### Three-Layer Structure
1. **raw/** - Source data models, one-to-one with source tables
   - Materialized as tables
   - Tagged by source system and refresh frequency (daily, weekly, monthly)
   - Schema: `brutos_*`

2. **intermediate/** - Business logic transformations
   - Default: ephemeral (not materialized)
   - Some exceptions materialized as tables (e.g., WhatsApp intermediate models)
   - Schema: `intermediario_*`

3. **marts/** - Final analytical models organized by business domain
   - Materialized as tables
   - Schema: Domain-specific (e.g., `projeto_*`, `saude_*`, `dashboard_*`)

### Domain Organization
Models are organized by health department domains:
- **core**: Master data dimensions and facts (e.g., `saude_dados_mestres`, `saude_linkage`)
- **dit**: Data and Technology Department projects (dashboards, clinical records, inventory)
- **subpav**: Undersecretary of Health Promotion projects (gestations, care lines, pharmacy)
- **subgeral**: General Undersecretary projects (CNES, sisreg, monitoring)
- **cie**: Strategic Information Center (disease alerts, vaccination, dashboards)
- **iplanrio**: Planning Institute projects (WhatsApp, PIC, patient)
- **ivisa**: Health Surveillance (establishments)
- **minha_saude**: My Health app (exams)

### Data Governance

#### Policy Tags
The project uses BigQuery policy tags for PII/sensitive data access control. Tags are defined in `dbt_project.yml` vars and applied to columns.

Two taxonomies:
- **TAG_PUBLICO_***: Free access tags (e.g., `TAG_PUBLICO_CPF`, `TAG_PUBLICO_NOME`)
- **TAG_***: Restricted access tags (e.g., `TAG_CPF`, `TAG_CNS`, `TAG_DADO_CLINICO`)

Tag categories:
- Identifiers: CPF, CNS, conselho_de_classe, data_nascimento, identidade, nome, nome_mae, nome_pai
- Contact: contato, email, endereco, telefone
- Other: dado_bancario, dado_clinico, dado_estoque

#### Data Labels
All models must have labels in `dbt_project.yml`:
- `dado_publico`: sim/nao - Is the data public?
- `dado_pessoal`: sim/nao - Does it contain personal data?
- `dado_sensivel`: sim/nao - Does it contain sensitive data (health, biometric, etc.)?
- `dominio`: Domain/area responsible (e.g., subpav, subgeral, historico_clinico, estoque)

#### Required Tests and Docs
All models require:
- At least 2 tests combining unique/not_null
- Documentation (description)

Configuration: `+required_tests: { "unique.*|not_null": 2 }`, `+required_docs: true`

### Source Systems
Major source systems (in `raw/`):
- **Prontuário Vitacare/Vitai/Sarah/ProntuaRio**: Electronic health records
- **CNES**: National Registry of Health Establishments
- **SIH/SIA/SIM**: National Health Information Systems
- **SISREG**: Regulation System
- **Minhasaude**: My Health app
- **iPlanRio**: Planning Institute systems
- **GAL**: Laboratory Management
- **SIPNI**: Immunization Information System

## Custom Macros

The `macros/` directory contains 50+ custom macros for data transformations. Notable ones include:

**Text cleaning/standardization**:
- `clean_bairro.sql`, `clean_cep.sql`, `clean_cidade.sql`: Geographic data standardization
- `add_accents_estabelecimento.sql`: Add accents to establishment names
- `capitalize_first_letter.sql`: Capitalize names
- `remove_person_description.sql`: Remove titles/suffixes from names

**Calculations**:
- `calculate_age.sql`: Age calculation
- `calculate_jaccard.sql`, `calculate_levenshtein.sql`: String similarity metrics
- `calculate_median.sql`: Median calculation

**Domain-specific**:
- `cdi_date_cleaning.sql`: CDI-specific date cleaning
- `avalia_protocolo_consultas_minimas.sql`: Evaluate consultation protocols

## Git Workflow

### Branch Naming
- Feature: `feat/<brief-description>`
- Fix: `fix/<issue>`

### Commit Format
Use semantic commits:
- `feat:<description>` - New feature
- `fix:<description>` - Bug fix
- `refact:<description>` - Refactoring
- `docs:<description>` - Documentation

### Before Opening PR
1. Run `dbt build` to ensure models compile and tests pass
2. Run Recce to verify data changes: `./tools/recce.sh`
3. Include Recce screenshot in PR description if applicable
4. Ensure pre-commit hooks pass

## Testing

Tests are in `tests/` directory with generic tests and mart-specific tests. Run tests:
```bash
# All tests
dbt test

# Failed tests are stored in gerenciamento__dbt_test_audit schema
# Test failures have severity: error
```

## Important Notes

- **Python Version**: Must use Python 3.10.x (NOT 3.11+)
- **Poetry Version**: 1.7.1
- **No Root Package**: Always use `poetry install --no-root`
- **Thread Count**: BigQuery profiles use 10 threads
- **Location**: All BigQuery datasets are in US region
- **Schema Naming**: Dev schemas include `DBT_USER` variable for isolation
- **Materialization**: raw = table, intermediate = ephemeral (with exceptions), marts = table
- **Query Comments**: Enabled with `dbt_bigquery_monitoring` for cost tracking
- **Elementary Integration**: Enabled in prod environment for monitoring (`gerenciamento__dbt_elementary` schema)

## Administrator

Project administrator: **Pedro Marques** ([@TanookiVerde](https://github.com/TanookiVerde))
