{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'equipe_tutela_coletiva_prazos'
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__equipe_tutela_coletiva') }}

),

prazo as (

    select
        area,
        assunto,
        data_da_entrada,
        ic,
        orgao,
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
        processo_rio,
        reiteracoes,
        sintese_da_solicitacao,
        status,
        status_sla,

        -- Data limite do prazo
        data_fim_sla as data_vencimento_prazo,

        -- dias para vencer
        date_diff(
            data_fim_sla,
            current_date(),
            day
        ) as dias_para_vencer,

        -- criando logica de antes e depois do prazo de vencimento
        case 
            -- caso nao tenha data_envio
            when status <> 'RESOLVIDO' then null
            when data_envio_orgao_solicitante_arquivamento <= data_fim_sla then 'Dentro do Prazo'
            else 'Fora do Prazo'
        end as situacao_prazo,
        -- calculando tempo de atendimento
        
        case 
            when status <> 'RESOLVIDO' 
                then null
            else date_diff(
                    date(data_envio_orgao_solicitante_arquivamento),
                    date(data_da_entrada),
                    day
        
        ) end as tempo_atendimento_dias
            


    from base

)

select *
from prazo
