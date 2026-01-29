{{ 
    config(
        materialized = 'table',
        schema = 'marts_cdi',
        alias = 'equipe_tutela_coletiva_cards_finais_v2'
    ) 
}}

with base as (

    select 
        case when 
            area is null then 'Não Aplicável' 
            else area 
        end as area,
        assunto,
        data_da_entrada,
        data_envio_orgao_solicitante_arquivamento,
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

        case 
            when ic is null then 'Não Aplicável' 
            else ic 
        end as ic,
        case 
            when orgao is null then 'Sem Informação' 
            else orgao 
        end as orgao,
        reiteracoes,
        sintese_da_solicitacao,
        status,
        case
        WHEN data_da_entrada IS NULL OR prazo_dias IS NULL
            THEN 'Ignorado'

        WHEN data_envio_orgao_solicitante_arquivamento IS NULL
            AND DATE_ADD(data_da_entrada, INTERVAL prazo_dias DAY) >= CURRENT_DATE()
            THEN 'Dentro do prazo'

        WHEN data_envio_orgao_solicitante_arquivamento IS NULL
            AND DATE_ADD(data_da_entrada, INTERVAL prazo_dias DAY) < CURRENT_DATE()
            THEN 'Fora do prazo'

        WHEN data_envio_orgao_solicitante_arquivamento <= DATE_ADD(data_da_entrada, INTERVAL prazo_dias DAY)
            THEN 'Dentro do prazo'

        ELSE 'Fora do prazo'
        END AS status_prazo,
        CASE
            WHEN data_envio_orgao_solicitante_arquivamento IS NOT NULL
                AND data_da_entrada IS NOT NULL
                THEN DATE_DIFF(
                data_envio_orgao_solicitante_arquivamento,
                data_da_entrada,
                DAY
                )
        END AS dias_atendimento,
    from {{ ref('int_cdi__equipe_tutela_coletiva') }}

)

select * from base