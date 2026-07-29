# AGENTS.md

Quick-start for AI agents working in `queries-rj-sms` — a dbt + BigQuery project for the Municipal Health Department of Rio de Janeiro (SMS/RJ).

For full developer guidance see `CLAUDE.md`. This file highlights only what an agent would likely miss.

---

## Setup (exact sequence)

```bash
# Python MUST be 3.10.x — NOT 3.11+
poetry shell
poetry install --no-root   # --no-root is mandatory; project has no installable package
dbt deps                   # install dbt packages after every packages.yml change
```

**Required env vars** — set before any `dbt` command:

| Var | Default | Notes |
|-----|---------|-------|
| `DBT_USER` | `dev_fantasma` | Injected into dev schema names for isolation |
| `HASH_SECRET` | **none** | **No fallback** — dbt fails immediately at compile without this |
| `DBT_PROFILES_DIR` | — | Point to repo root — `profiles.yml` lives there, not in `~/.dbt/` |

```bash
export DBT_PROFILES_DIR=$(pwd)
export HASH_SECRET=$(openssl rand -hex 32)
export DBT_USER=your_name
```

---

## Key commands

```bash
# Build (run + test) — use before opening a PR
dbt build

# Run a single model
dbt run -s "model_name"

# Run model + all downstream
dbt run -s "model_name+"

# Test a single model
dbt test -s "model_name"

# Docs
dbt docs generate && dbt docs serve   # localhost:8080

# Python lint (black + isort + flake8)
task lint

# SQL lint (requires valid credentials)
sqlfluff lint models/path/to/model.sql

# Pre-commit
pre-commit run --all-files
```

### Recce (before PR merge — compare branch vs. prod)

```bash
./tools/recce.sh            # current branch vs. prod, opens localhost:8000
./tools/recce.sh -b feat/x  # specific branch
./tools/recce.sh --dry-run  # show commands without running dbt
```

**Quirk**: `recce.sh` runs `git checkout master` then back to your branch. Working tree must be clean.

---

## Architecture

Three-layer model directory:

| Layer | Path | Materialization | Schema prefix |
|-------|------|-----------------|---------------|
| Raw | `models/raw/` | table | `brutos_*` |
| Intermediate | `models/intermediate/` | ephemeral (default) | `intermediario_*` |
| Marts | `models/marts/` | table | domain-specific |

- **Intermediate exceptions**: WhatsApp models are materialized as `table`, not ephemeral.
- **Disabled models**: `raw/sisvisa/` and `marts/ivisa/empreendimentos_cariocas/` are globally disabled (`+enabled: false`).
- All BigQuery datasets are in the **US region** — cross-region queries will fail.

Domain directories under `intermediate/` and `marts/`: `cie/`, `core/`, `dit/`, `iplanrio/`, `ivisa/`, `subgeral/`, `subpav/`, plus `minha_saude/`, `ipp/`, `subhue/` in marts.

---

## Data governance — required for every new model

### Labels (in `dbt_project.yml`)
```yaml
+labels:
  dado_publico: "sim"       # or "nao"
  dado_pessoal: "sim"       # or "nao"
  dado_sensivel: "sim"      # or "nao"
  dominio: "subpav"         # responsible area
```

### Tests and docs
- **At least 2 tests** combining `unique` or `not_null` — enforced by `dbt_meta_testing`.
- **Description required** (`+required_docs: true`) — build fails without it.
- All test failures are `severity: error` — no warnings.
- Failed rows stored in `gerenciamento__dbt_test_audit`.

### Policy tags (PII columns)
Apply via `policy_tags` in `schema.yml` using dbt vars, e.g.:
```yaml
columns:
  - name: cpf
    policy_tags: ["{{ var('TAG_CPF') }}"]
```
Tag vars are defined in `dbt_project.yml`. Two tiers: `TAG_PUBLICO_*` (free access) and `TAG_*` (restricted).

### Anonymization
Use the `anonimize` macro for PII — it requires `HASH_SECRET` at compile time.

---

## Non-obvious quirks

- **`profiles.yml` is in the repo root** — CI uses `--profiles-dir .`. Set `DBT_PROFILES_DIR=$(pwd)` locally.
- **Elementary is prod-only** — `+enabled: "{{ target.name in ['prod'] }}"` — does nothing on dev/ci.
- **`dbt_meta_testing` breaks builds** — adding a model without description or 2+ tests causes `dbt build` to fail.
- **CI uses pip, not Poetry** — `dbt-compile.yaml` installs dbt directly with pip; this is intentional.
- **SQL linter CI is manual-only** (`workflow_dispatch`) — SQLFluff does not run automatically on PRs.
- **Query comments are BigQuery job labels** (via `dbt_bigquery_monitoring`) — used for cost tracking; don't remove.
- **`package-lock.yml`** (not `packages.yml`) is used as the CI cache key for `dbt_packages/`.
- **SQLFluff uses dbt templater** — needs valid credentials to lint; `sqlfluff lint` will fail without them.
- **`dbt-command.yaml`** posts a suggested `dbt run --select model_name+ --full-refresh` as a PR comment — informational only, does not execute dbt.

---

## Git conventions

Branch names: `feat/<description>` or `fix/<issue>`

Commits (semantic, no scope):
```
feat:<description>
fix:<description>
refact:<description>
docs:<description>
```

PR checklist: `dbt build` passes → Recce run → pre-commit passes → Recce screenshot in PR if data changed.

---

## Custom macros (notable)

- `anonimize` — PII hashing (needs `HASH_SECRET`)
- `validate_cpf`, `validate_cns` — Brazilian document validation
- `parse_date`, `parse_datetime`, `parse_time` — date/time parsing
- `clean_bairro`, `clean_cep`, `clean_cidade` — geographic field standardization
- `calculate_jaccard`, `calculate_levenshtein` — string similarity
- `calculate_age` — age from date of birth
- `padronize_telefone`, `padronize_cep` — contact normalization
- `get_last_partition_date` — incremental model partition helper
- `proper_br` — Brazilian Portuguese proper casing

Full list: `macros/` (53+ files).
