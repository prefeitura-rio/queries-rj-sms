param (
    [string]$BranchName,
    [string]$Target = "dev", # Define o valor padrão como 'dev'
    [string]$FullRefresh = $null
)

if (-not $BranchName) {
    Write-Host "Uso: .\script.ps1 -BranchName <nome-da-branch> [-Target <dev|ci>] [-FullRefresh full_refresh]"
    exit 1
}

if ($FullRefresh -eq "full_refresh") {
    $FullRefreshFlag = "--full-refresh"
} else {
    $FullRefreshFlag = ""
}

Write-Host ""
Write-Host ">>>>> Checkout na branch 'master'"
git checkout master
git pull

Write-Host ""
Write-Host ">>>>> Gerando artefatos para o ambiente base em '.state/'"
dbt docs generate --target prod --target-path .state/

Write-Host ""
Write-Host ">>>>> Checkout na branch '$BranchName'"
git checkout $BranchName

Write-Host ""
Write-Host ">>>>> Executando dbt com target '$Target'"
dbt run -s "state:modified+" --defer --state .state/ --target $Target $FullRefreshFlag
