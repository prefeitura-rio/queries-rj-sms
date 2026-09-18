{{
    config(
        alias="fatura_recursos", 
        partition_by={"field": "tempo_exportacao", "data_type": "timestamp", "granularity": "month"},
    )
}}

select
    service.description as servico_nome,
    sku.description as servico_sku,
    project.id as projeto_id,
    project.name as projeto_nome,
    cost as custo,
    currency as moeda,
    currency_conversion_rate as taxa_conversao_moeda,
    usage_start_time as tempo_inicio_uso,
    export_time as tempo_exportacao
from {{ source('brutos_faturamento_nuvem_staging', 'gcp_billing_export_resource_v1_01BAED_6E64B4_606041') }}