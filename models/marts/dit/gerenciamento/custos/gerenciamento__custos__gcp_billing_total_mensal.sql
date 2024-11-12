{{
    config(
        schema="gerenciamento__custos",
        alias="gcp_billing_total_mensal",
        materialized="incremental",
        tags=["daily"],
    )
}}

with
    source as (
        select * from `rj-sms-dev.billing_reports.2024_*`
        where safe_cast(invoice.month as int64) >= 202408
    ),

    gcp_billing_total_mensal as (
        select
            project.name as projeto,
            service.description as servico,
            invoice.month,
            left(invoice.month, 4) || '-' || right(invoice.month, 2) as competencia,
            round(sum(cost), 0) as custo_brl
        from source
        group by 1, 2, 3
        order by 1, 3 desc
    )

select *
from gcp_billing_total_mensal
{% if is_incremental() %}
    where competencia > (select max(competencia) from {{ this }})
{% endif %}
