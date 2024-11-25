# Verifica se um argumento foi passado
param(
    [string]$branchName
)

if (-not $branchName) {
    Write-Host "Uso: .\script.ps1 <nome-da-branch>"
    exit 1
}

Write-Host ""
Write-Host ">>>>> Checkout na branch 'master'"
git checkout master

Write-Host ""
Write-Host ">>>>> Gerando artefatos para o ambiente base em 'target-base'"
dbt docs generate --target prod --target-path target-base/

Write-Host ""
Write-Host ">>>>> Checkout na branch '$branchName'"
git checkout $branchName

Write-Host ""
Write-Host ">>>>> Executando dbt e gerando artefatos para os ambientes de trabalho atuais"
dbt run
dbt docs generate

Write-Host ""
Write-Host ">>>>> Iniciando o servidor Recce"
recce server
