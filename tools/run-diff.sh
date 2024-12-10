#!/bin/bash

# Verifica se pelo menos um argumento foi passado
if [ $# -lt 1 ]; then
  echo "Uso: $0 <BranchName> [Target=dev] [FullRefresh]"
  exit 1
fi

# Captura parâmetros
BranchName="$1"
Target="${2:-dev}" # Define o valor padrão como 'dev'
FullRefresh="${3:-}"

# Configura a flag full_refresh
if [ "$FullRefresh" == "full_refresh" ]; then
  FullRefreshFlag="--full-refresh"
else
  FullRefreshFlag=""
fi

# Função para executar comandos e verificar falhas manualmente
run_command() {
  local command="$1"
  echo "Executing: $command"
  eval "$command"
  if [ $? -ne 0 ]; then
    echo "ERROR: Command failed: $command"
    echo "Terminating process."
    return 1
  fi
}

# Etapas do script
echo ""
echo "STEP 1"
echo ">>>> CHECKING OUT 'master' BRANCH"
run_command "git checkout master" 
run_command "git pull" 

echo ""
echo "STEP 2"
echo ">>>> GENERATING STATE '.state/' BASED ON 'master' BRANCH"
run_command "dbt docs generate --target prod --target-path .state/" 

echo ""
echo "STEP 3"
echo ">>>> CHECKING OUT BRANCH '$BranchName'"
run_command "git checkout $BranchName" 

echo ""
echo "STEP 4"
echo ">>>> EXECUTING DBT MATERIALIZATIONS"
echo ">>>>>>> TARGET: $Target"
echo ">>>>>>> FULL REFRESH: $FullRefreshFlag"
run_command "dbt run -s 'state:modified+' --defer --state .state/ --target $Target $FullRefreshFlag"

echo "Script completed successfully."
