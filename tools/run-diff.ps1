# Verifica se um argumento foi passado
if (-not $args[0]) {
    Write-Host "Uso: .\script.ps1 <nome-da-branch>"
    exit 1
}

$BranchName = $args[0]

Write-Host ""
Write-Host ">>>>> Checkout na branch 'master'"
git checkout master

Write-Host ""
Write-Host ">>>>> Gerando artefatos para o ambiente base em 'target-base'"
dbt docs generate --target prod --target-path .target-base/

Write-Host ""
Write-Host ">>>>> Checkout na branch '$BranchName'"
git checkout $BranchName

Write-Host ""
Write-Host ">>>>> Executando dbt"
dbt run -s "state:modified+" --defer --state .target-base/ --target ci
