<#
    Loads ../.env (DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN)
    then runs dbt against this project with dbt/profiles.yml as the profile dir.

    Usage:
        ./dbt/run.ps1 debug
        ./dbt/run.ps1 run
        ./dbt/run.ps1 test
        ./dbt/run.ps1 build
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("debug", "run", "test", "build")]
    [string]$Command
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot ".env"

if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$') {
            $name = $matches[1]
            $value = $matches[2].Trim('"').Trim("'")
            [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}
else {
    Write-Warning "$envFile not found - dbt will fail unless DATABRICKS_HOST/DATABRICKS_HTTP_PATH/DATABRICKS_TOKEN are already set in this shell."
}

dbt $Command --project-dir $PSScriptRoot --profiles-dir $PSScriptRoot
