import subprocess
import sys
from typing import Optional

def run_command(command, shell=False):
    """Executa um comando no terminal e exibe informações detalhadas em caso de erro."""
    try:
        print(f"Executing: {' '.join(command)}")
        result = subprocess.run(
            command,
            shell=shell,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"\nERROR IN COMMAND: {' '.join(command)}")
        print("Command output (stdout):")
        print(e.stdout)
        print("\nCommand error (stderr):")
        print(e.stderr)
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <branch-name> [target] [full_refresh]")
        sys.exit(1)

    branch_name = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else "dev"
    full_refresh = sys.argv[3] if len(sys.argv) > 3 else ""

    # Determinar flag de full_refresh
    full_refresh_flag = "--full-refresh" if full_refresh == "full_refresh" else ""

    print("\nSTEP 1")
    print(">>>> CHECKING OUT 'master' BRANCH")
    run_command(["git", "checkout", "master"])
    run_command(["git", "pull"])

    print("\nSTEP 2")
    print(">>>> GENERATING STATE '.state/' BASED ON 'master' BRANCH")
    run_command(["dbt", "docs", "generate", "--target", "prod", "--target-path", ".state/"])

    print("\nSTEP 3")
    print(f">>>> CHECKING OUT BRANCH '{branch_name}'")
    run_command(["git", "checkout", branch_name])

    print("\nSTEP 4")
    print(">>>> EXECUTING DBT MATERIALIZATIONS")
    print(f">>>>>> TARGET: {target}")
    print(f">>>>>> FULL REFRESH: {full_refresh_flag}")
    run_command([
        "dbt", "run",
        "-s", "state:modified+",
        "--defer", "--state", ".state/",
        "--target", target,
        full_refresh_flag
    ])

    print("\nENDING")

if __name__ == "__main__":
    main()
