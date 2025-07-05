#!/bin/bash

# Cores ANSI
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # Sem cor

# Verifica se um argumento foi passado
if [ -z "$1" ]; then
  echo -e "${YELLOW}Uso: $0 <nome-da-branch>${NC}"
  exit 1
fi

BRANCH_NAME=$1

echo ""
echo -e "${BLUE}>>>>> Checkout na branch 'master'${NC}"
git checkout master
git pull

echo ""
echo -e "${GREEN}>>>>> Gerando artefatos para o ambiente base em 'target-base'${NC}"
dbt docs generate --target prod --target-path target-base/

echo ""
echo -e "${BLUE}>>>>> Checkout na branch '$BRANCH_NAME'${NC}"
git checkout "$BRANCH_NAME"

echo ""
echo -e "${GREEN}>>>>> Executando dbt e gerando artefatos para os ambientes de trabalho atuais${NC}"
dbt build -s "state:modified+" --defer --full-refresh --state target-base/
dbt docs generate

echo ""
echo -e "${YELLOW}>>>>> Iniciando o servidor Recce${NC}"
recce server