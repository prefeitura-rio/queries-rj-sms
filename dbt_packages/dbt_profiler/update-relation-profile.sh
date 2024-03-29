#!/usr/bin/env bash
relation_name=$1
schema=$2

if test -z "$relation_name" || test -z "$schema"
then
    echo "usage: update-relation-profile.sh RELATION_NAME SCHEMA"
    exit 1
fi

RUN_OPERATION_OUTPUT=$(dbt run-operation print_profile_docs --args '{"schema": "'$schema'", "relation_name": "'$relation_name'", "docs_name": "dbt_profiler_results__'$schema'_'$relation_name'"}')

# Extract docs block from stdout
PROFILE=$(echo "$RUN_OPERATION_OUTPUT" | awk '/{%.docs.*%}$/{flag=1} flag; /{%.enddocs.%}/{flag=0}')

output_dir=docs/dbt_profiler/$schema
output_path=$output_dir/$relation_name.md
mkdir -p $output_dir
echo "$PROFILE" > $output_path

# Print file path and contents
echo $output_path
cat $output_path