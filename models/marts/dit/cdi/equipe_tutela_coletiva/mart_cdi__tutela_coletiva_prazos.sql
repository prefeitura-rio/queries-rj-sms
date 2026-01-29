{{ 
    config(
        materialized = 'table',
        schema = 'marts_cdi',
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

        -- Dias para vencer (positivo = no prazo, negativo = vencido)
        date_diff(
            data_fim_sla,
            current_date(),
            day
        ) as dias_para_vencer,

        CASE
            WHEN data_da_entrada IS NULL 
                OR prazo_dias IS NULL 
                OR data_envio_orgao_solicitante_arquivamento IS NULL
                THEN 'Sem Prazo'

            WHEN DATE(data_envio_orgao_solicitante_arquivamento)
                <= DATE_ADD(DATE(data_da_entrada), INTERVAL prazo_dias DAY)
                THEN 'Dentro do Prazo'

            ELSE 'Fora do Prazo'
        END AS situacao_prazo,


        date_diff(
            date(data_envio_orgao_solicitante_arquivamento),
            date(data_da_entrada),
            day
        ) as tempo_atendimento_dias,
        case
            when status is null
                then 'Pendente'

            when upper(status) = 'RESOLVIDO'
                then 'Resolvido'

            when upper(status) like 'PENDENTE:%ENCERRADO%'
                then 'Pendente - Prazo encerrado'

            when upper(status) like 'PENDENTE:%15%'
                then 'Pendente - Prazo menor que 15 dias'

            when upper(status) like 'PENDENTE%'
                then 'Pendente'

            else 'Outros'
        end as status_padronizado


    from base

)

select *
from prazo
