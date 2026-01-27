{{ 
    config(
        materialized = 'table',
        schema = 'intermediario_cdi',
        alias = 'int_cdi__tutela_equipe_coletiva_v1'
    ) 
}}

with base as (

    select *
    from {{ ref('raw_cdi__tutela_coletiva') }}

),

typed as (

    select
        processo_rio,
        num_oficio,
        orgao,
        assunto,
        area,
        ic,
        status,
        unidade,
        orgao_para_subsidiar,
        exigencia,
        sintese_da_solicitacao,
        observacoes,
        pa,
        referencia,
        no_documento,
        sei,
        reiteracoes,
        oficio_sms,

        cast(data_da_entrada as date) as data_da_entrada,
        cast(vencimento as date) as vencimento,
        cast(data_de_saida as date) as data_de_saida,
        cast(data_envio_orgao_solicitante_arquivamento as date) 
            as data_envio_orgao_solicitante_arquivamento,

        safe_cast(prazo_dias as int64) as prazo_dias

    from base

),

sla as (

    select
        *,
        date_add(
            data_da_entrada,
            interval prazo_dias day
        ) as data_fim_sla

    from typed

),

status as (

    select
        *,
        case
            when data_da_entrada is null
              or prazo_dias is null
              or data_envio_orgao_solicitante_arquivamento is null
                then null

            when data_envio_orgao_solicitante_arquivamento <= data_fim_sla
                then 'Dentro do prazo'

            else 'Fora do prazo'
        end as status_sla

    from sla

)

select *
from status
