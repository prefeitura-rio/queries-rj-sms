#!/bin/bash

# Verifica se um argumento foi passado
if [ -z "$1" ]; then
  echo "Uso: $0 <nome-da-branch>"
  exit 1
fi

BRANCH_NAME=$1

echo ""
echo ">>>>> Checkout na branch 'master'"
git checkout master
git pull

echo ""
echo ">>>>> Gerando artefatos para o ambiente base em 'target-base'"
dbt docs generate --target prod --target-path target-base/

echo ""
echo ">>>>> Checkout na branch '$BRANCH_NAME'"
git checkout "$BRANCH_NAME"

echo ""
echo ">>>>> Executando dbt e gerando artefatos para os ambientes de trabalho atuais"
dbt build -s "state:modified+" --defer --state target-base/
dbt docs generate

echo ""
echo ">>>>> Iniciando o servidor Recce"
recce server