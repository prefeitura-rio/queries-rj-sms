-- Custos do BigQuery abertos por execução
-- Ref: https://cloud.google.com/bigquery/docs/information-schema-jobs#compare_on-demand_job_usage_to_billing_data

{{
    config(
        alias="billing",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "billing_date",
            "data_type": "date",
            "granularity": "month",
        },
        tags=["daily"],
    )
}}

{% set first_day_of_month = "date_trunc(current_date('America/Sao_Paulo'), month)" %}

with
    query_params as (
        select
            date '2024-08-01' as start_date,  -- inclusive
            current_date() as end_date,  -- inclusive
    ),

    -- SOURCES

    usage_rj_sms as (
        select * from `rj-sms`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    ),

    usage_rj_sms_dev as (
        select * from `rj-sms-dev`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    ),

    usage_rj_sms_sandbox as (
        select * from `rj-sms-sandbox`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    ),

    usuarios_bigquery as (
        select * from {{ ref("raw_sheets__usuarios_bigquery") }}
    ),

    -- UNION ALL

    all_usage as (
        select * from usage_rj_sms
        union all
        select * from usage_rj_sms_dev
        union all
        select * from usage_rj_sms_sandbox
    ),

    -- BILLING INFORMATION

    all_usage_with_multiplier as (
        select
            project_id,
            job_id,
            user_email,
            job_type,
            query,
            state,
            destination_table.project_id as destination_project_id,
            destination_table.dataset_id as destination_dataset_id,
            destination_table.table_id as destination_table_id,
            error_result,
            creation_time,
            extract(date from end_time at time zone 'PST8PDT') as billing_date,  -- Jobs are billed by end_time in PST8PDT timezone, regardless of where the job ran.
            total_bytes_processed / 1024 / 1024 / 1024 / 1024 as total_tib_processed,
            total_bytes_billed / 1024 / 1024 / 1024 / 1024 as total_tib_billed,
            case
                statement_type
                when 'SCRIPT'
                then 0
                when 'CREATE_MODEL'
                then 50 * 6.25
                else 6.25
            end as multiplier,
        from all_usage
    ),

    cost_added as (
        select
            *,
            total_tib_billed * multiplier as estimated_charge_in_usd,
            total_tib_billed as estimated_usage_in_tib,
            -- Inline logic from ismaybeusingrowlevelsecurity and isbillable functions
            (
                -- Logic for isbillable: You aren't charged for queries that return an error.
                -- Error result is considered billable if it’s null or if the error reason is "stopped"
                job_type = 'QUERY'
                and total_tib_billed is null
                and (
                    error_result is null  -- Not billed for errors
                    or error_result.reason = 'stopped'  -- Not billed for stopped queries
                )
            ) as job_using_row_level_security,
        from all_usage_with_multiplier
    ),

    emails_added as (
        select
            cost_added.*,
            usuarios_bigquery.* except(email)
        from cost_added
            left join usuarios_bigquery on cost_added.user_email = usuarios_bigquery.email
    ),

    -- FINAL

    final as (
        select
            project_id,
            job_id,
            creation_time,
            struct(
                user_email as email,
                nome as name,
                forma as type,
                organizacao as organization,
                subsecretaria as subsecretary,
                setor_da_sms as sms_sector,
                nucleo as team
            ) as user,
            job_type,
            query,
            state,
            struct(
                destination_project_id as project_id,
                regexp_replace(
                destination_dataset_id,
                r'(dev_fantasma__|diego__|miloskimatheus__|pedro__|thiago__|vit__)',
                ''
                ) as dataset_id,
                destination_table_id as table_id
            ) as destination,
            total_tib_processed,
            error_result,
            billing_date,
            estimated_usage_in_tib as billing_estimated_usage_in_tib,
            estimated_charge_in_usd as billing_estimated_charge_in_usd,
            job_using_row_level_security,
        from emails_added
    )

select *
from final
{% if is_incremental() %} where billing_date >= {{ first_day_of_month }} {% endif %}