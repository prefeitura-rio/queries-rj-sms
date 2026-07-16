{{
    config(
        schema = "intermediario_cdi",
        alias = "tutela_coletiva",
        materialized = "table",
        meta={"owner": "karen"}
    )
}}

with tutela_coletiva_2025 as (

    select
        processo_rio,
        oficio,
        orgao,
        reiteracoes,
        ic,
        pa,
        referencia,
        no_documento,
        data_of,
        data_entrada,
        prazo_dias,
        vencimento,
        assunto,
        solicitacao,
        sintese_solicitacao,
        unidade,
        area,
        data_saida,
        sei_status,
        orgao_para_subsidiar,
        retorno,
        exigencia,
        retorno_2,
        oficio_sms,
        data_oficio,
        data_envio_orgao_solicitante_arquivamento,
        observacoes,
        status

    from {{ ref('raw_cdi__tutela_coletiva_2025') }}

),

tutela_coletiva_2026 as (

    select
        processo_rio,
        oficio,
        orgao,
        reiteracoes,
        ic,
        pa,
        referencia,
        no_documento,
        data_of,
        data_entrada,
        prazo_dias,
        vencimento,
        assunto,
        cast(null as string) as solicitacao,
        sintese_solicitacao,
        unidade,
        area,
        data_saida,
        sei_status,
        orgao_para_subsidiar,
        retorno,
        exigencia,
        cast(null as string) as retorno_2,
        oficio_sms,
        cast(null as date) as data_oficio,
        data_envio_orgao_solicitante_arquivamento,
        observacoes,
        status

    from {{ ref('raw_cdi__tutela_coletiva_2026') }}

),

base as (

    select * from tutela_coletiva_2025

    union all

    select * from tutela_coletiva_2026

),

calc as (

    select
        *,

        date_add(
            data_entrada,
            interval prazo_dias day
        ) as data_fim_sla,

        case
            when upper(status) <> 'RESOLVIDO' then null

            when data_envio_orgao_solicitante_arquivamento <= date_add(data_entrada, interval prazo_dias day)
                then 'Dentro do prazo'

            else 'Fora do prazo'
        end as status_sla

    from base

)

select *
from calc
where processo_rio is not null
  and data_entrada is not null