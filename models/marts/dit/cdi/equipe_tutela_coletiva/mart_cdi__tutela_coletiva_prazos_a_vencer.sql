{{ 
    config(
        materialized = 'table',
        schema = 'marts_cdi',
        alias = 'equipe_tutela_coletiva_demandas_prazos_a_vencer'
    ) 
}}


with base as (

    select
        processo_rio,
        assunto,
        data_da_entrada,
        area,
        orgao,
        ic,
        reiteracoes,
        sintese_da_solicitacao,
        case 
            when status = "" then "Sem Status"
            else status
        end as status,
        
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

        -- saldo de dias em relação ao vencimento
        date_diff(
            date_add(data_da_entrada, interval prazo_dias day),
            current_date(),
            day
        ) as dias_para_vencer,
        -- data de vencimento
        date_add(data_da_entrada, interval prazo_dias day) as data_vencimento_prazo

    from {{ ref('int_cdi__equipe_tutela_coletiva') }}

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
