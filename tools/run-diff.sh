#!/bin/bash
set -euo pipefail

# Verifica se pelo menos um argumento foi passado
if [ -z "$1" ]; then
  echo "Uso: $0 <nome-da-branch> [target=dev] [full_refresh]"
  exit 1
fi

BRANCH_NAME=$1
TARGET=${2:-dev} # Define o valor padrão como 'dev'
FULL_REFRESH=""

# Configura a flag full_refresh se o terceiro parâmetro for passado
if [ "$3" == "full_refresh" ]; then
  FULL_REFRESH="--full-refresh"
fi

echo ""
echo ">>>>> Checkout na branch 'master'"
git checkout master
git pull

echo ""
echo ">>>>> Gerando artefatos para o ambiente base em 'target-base'"
dbt docs generate --target prod --target-path .state/

echo ""
echo ">>>>> Checkout na branch '$BRANCH_NAME'"
git checkout "$BRANCH_NAME"

echo ""
echo ">>>>> Executando dbt com target '$TARGET'"
dbt run -s "state:modified+" --defer --state .state/ --target "$TARGET" $FULL_REFRESH
