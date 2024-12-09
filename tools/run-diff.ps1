param (
    [Parameter(Mandatory = $true)]
    [string]$BranchName,

    [string]$Target,

    [string]$FullRefresh
)

# Define valores padrão para parâmetros opcionais
if (-not $Target) {
    $Target = "dev"
}

if ($FullRefresh -eq "full_refresh") {
    $FullRefreshFlag = "--full-refresh"
} else {
    $FullRefreshFlag = ""
}

function Run-Command {
    param (
        [string]$Command
    )
    try {
        & $Command
    } catch {
        Write-Host "ERRO NO COMANDO: $Command."
        Write-Host "MENSAGEM DE ERRO: $($_.Exception.Message)"
        exit 1
    }
}

$ErrorActionPreference = "Stop"

Write-Host "ETAPA 1"
Write-Host ">>>> CHECKOUT NA BRANCH 'master'"
Run-Command "git checkout master"
Run-Command "git pull"

Write-Host "ETAPA 2"
Write-Host ">>>> GERANDO ESTADO '.state/' COM BASE NA BRANCH 'master'"
Run-Command "dbt docs generate --target prod --target-path .state/"

Write-Host "ETAPA 3"
Write-Host ">>>> CHECKOUT NA BRANCH '$BranchName'"
Run-Command "git checkout $BranchName"

Write-Host "ETAPA 4"
Write-Host ">>>> EXECUTANDO MATERIALIZAÇÕES DO DBT"
Write-Host ">>>>>>> TARGET: $Target"
Write-Host ">>>>>>> FULL REFRESH: $FullRefreshFlag"
Run-Command "dbt run -s 'state:modified+' --defer --state .state/ --target $Target $FullRefreshFlag"
