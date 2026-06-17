{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'tutela_coletiva_prazos_a_vencer',
        meta={"owner": "karen"}
    ) 
}}

with base as (

    select
        processo_rio,
        assunto,
        data_entrada,
        area,
        orgao,
        ic,
        reiteracoes,
        sintese_solicitacao,
        status,
        prazo_dias,

        case
            when upper(orgao) like 'MPF%' then 'MPF'
            when upper(orgao) like 'MPRJ%'
              or upper(orgao) like 'PJTCPICAP%'
              or upper(orgao) like 'CAO%' then 'MPRJ'
            when upper(orgao) like '%COSAU%' then 'COSAU'
            when upper(orgao) like 'DPU%' then 'DPU'
            when upper(orgao) like 'DGH%' then 'DGH'
            else 'Outros'
        end as grupo_orgao,

        date_diff(
            date_add(data_entrada, interval prazo_dias day),
            current_date(),
            day
        ) as dias_para_vencer,

        date_add(data_entrada, interval prazo_dias day) as data_vencimento_prazo

    from {{ ref('int_cdi__tutela_coletiva') }}

),

final as (

    select
        *,
        case
            when dias_para_vencer < 0
                then concat('Vencido há ', abs(dias_para_vencer), ' dias')

            when dias_para_vencer = 0
                then 'Vence hoje'

            when dias_para_vencer = 1
                then 'Vence amanhã'

            when dias_para_vencer = -1
                then 'Venceu ontem'

            when dias_para_vencer > 1
                then concat('Vence em ', dias_para_vencer, ' dias')
        end as situacao_vencimento

    from base
)

select *
from final