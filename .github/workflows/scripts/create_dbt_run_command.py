# -*- coding: utf-8 -*-
import sys
from pathlib import Path
from typing import List


message_id = 0

def log(message: str):
    """
    Logs a message to the output of a GitHub Action.
    """
    message = message.replace("\n", "%0A")
    print(f"pr-message={message} >> $GITHUB_OUTPUT")


if __name__ == "__main__":
    # Assert arguments.
    if len(sys.argv) not in [2, 3]:
        print(f"Usage: python {sys.argv[0]} <changed_files> [--write-to-file]")

    # Write to file?
    write_to_file = "--write-to-file" in sys.argv

    # Get modified files
    changed_files: List[str] = sys.argv[1].split(" ")
    print("These are all the changed files:")
    for file_ in changed_files:
        print(f"\t- {file_}")

    # Filter out non-models
    changed_files = [
        file_
        for file_ in changed_files
        if file_.endswith(".sql")
        and file_.startswith("models")
        and Path(file_).exists()
    ]
    print("We're interested in these files:")
    for file_ in changed_files:
        print(f"\t- {file_}")

    # Start a PR message
    message = "### Modelos Modificados\n\n"

    # Format a message for the files that depend on the exported declarations.
    if len(changed_files) > 0:
        message += "**Os seguintes modelos foram modificados:"
        for file_ in changed_files:
            message += f"\n\t- `{file_}`"
        message += "\n\n"

    # Create DBT run command
    models = []
    for file_ in changed_files:
        file_without_extension = file_.replace(".sql", "")
        model_name = file_without_extension[::-1].split("/", 1)[0][::-1]
        model_name = f"{model_name}+"
        models.append(model_name)
    
    model_sequence = ' '.join(models)
    command = f"dbt run --select {model_sequence} --full-refresh"

    message += "### Comando para rodar DBT\n\n"
    message += f"```bash\n{command}\n```"

    print(message)
    log(command)
