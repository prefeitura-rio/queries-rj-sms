param (
    [Parameter(Mandatory = $true)]
    [string]$BranchName,

    [string]$Target,

    [string]$FullRefresh
)

# Define default values for optional parameters
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
        Write-Host "Executing: $Command"
        Invoke-Expression $Command
        if ($LASTEXITCODE -ne 0) {
            throw "The command failed with exit code $LASTEXITCODE."
        }
    } catch {
        Write-Host "ERROR IN COMMAND: $Command. TERMINATING EXECUTION."
        exit 1
    }
}

# Configure "fail fast" mode
$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "STEP 1"
Write-Host ">>>> CHECKING OUT 'master' BRANCH"
Run-Command "git checkout master"
Run-Command "git pull"

Write-Host ""
Write-Host "STEP 2"
Write-Host ">>>> GENERATING STATE '.state/' BASED ON 'master' BRANCH"
Run-Command "dbt docs generate --target prod --target-path .state/"

Write-Host ""
Write-Host "STEP 3"
Write-Host ">>>> CHECKING OUT BRANCH '$BranchName'"
Run-Command "git checkout $BranchName"

Write-Host ""
Write-Host "STEP 4"
Write-Host ">>>> EXECUTING DBT MATERIALIZATIONS"
Write-Host ">>>>>>> TARGET: $Target"
Write-Host ">>>>>>> FULL REFRESH: $FullRefreshFlag"
Run-Command "dbt run -s 'state:modified+' --defer --state .state/ --target $Target $FullRefreshFlag"

Write-Host "ENDING"