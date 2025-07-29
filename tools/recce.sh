#!/bin/bash

# Cores ANSI
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # Sem cor

# Inicializa variáveis
BRANCH_NAME=""
TARGET_ENV="dev"  # valor default
DRY_RUN=false

# Obtém a branch atual como default
CURRENT_BRANCH=$(git branch --show-current 2>/dev/null)
if [ -z "$CURRENT_BRANCH" ]; then
  echo -e "${RED}Erro: Não foi possível determinar a branch atual. Certifique-se de estar em um repositório git.${NC}"
  exit 1
fi

# Processa argumentos
while [[ $# -gt 0 ]]; do
  case $1 in
    --branch|-b)
      BRANCH_NAME="$2"
      shift 2
      ;;
    --target|-t)
      TARGET_ENV="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      echo -e "${RED}>>>>> MODO DRY-RUN ATIVADO - dbt build será pulado${NC}"
      shift
      ;;
    *)
      # Para compatibilidade, se o primeiro argumento não for uma opção, assume que é a branch
      if [ -z "$BRANCH_NAME" ]; then
        BRANCH_NAME="$1"
        shift
      else
        echo -e "${RED}Argumento desconhecido: $1${NC}"
        exit 1
      fi
      ;;
  esac
done

# Verifica se a branch foi especificada, senão usa a branch atual
if [ -z "$BRANCH_NAME" ]; then
  BRANCH_NAME="$CURRENT_BRANCH"
  echo -e "${BLUE}>>>>> Usando branch atual: '$BRANCH_NAME'${NC}"
fi

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
if [ "$DRY_RUN" = true ]; then
  echo -e "${YELLOW}>>>>> [DRY-RUN] Pulando dbt build - seria executado: dbt build -s \"state:modified+\" --target $TARGET_ENV --defer --full-refresh --state target-base/${NC}"
else
  echo -e "${GREEN}>>>>> Executando dbt e gerando artefatos para os ambientes de trabalho atuais${NC}"
  dbt build -s "state:modified+" --target $TARGET_ENV --defer --full-refresh --state target-base/
fi

echo ""
echo -e "${GREEN}>>>>> Gerando documentação${NC}"
dbt docs generate --target $TARGET_ENV

echo ""
echo -e "${YELLOW}>>>>> Iniciando o servidor Recce${NC}"
recce server