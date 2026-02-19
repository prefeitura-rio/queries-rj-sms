{{ 
    config(
        materialized='table',
        schema='marts_cdi',
        alias='mart_cdi__equipe_tutela_individual_cards'
    ) 
}}

with base as (

    select
    * 
    from {{ ref('int_cdi__equipe_tutela_individual') }}

),

tratado as (

    select
        *,

        case
            when upper(orgao) like 'MP%' then 'Ministério Público'
            when upper(orgao) like 'DP%' then 'Defensoria Pública'
            else 'Outros'
        end as orgao_mp_dp,
        classificacao_idade as faixa_idade,
        case
            when regexp_contains(upper(sexo), r'M') 
                and regexp_contains(upper(sexo), r'F')
                then 'Ambos os gêneros'

            when upper(trim(sexo)) = 'F'
                then 'Feminino'

            when upper(trim(sexo)) = 'M'
                then 'Masculino'

            else 'Não informado'
        end as genero_tratado,


        -- Data de vencimento
        date_add(data_de_entrada, interval prazo_dias day) as data_vencimento,

        CASE
        WHEN data_do_sms_ofi IS NOT NULL
            AND data_de_entrada IS NOT NULL
            THEN DATE_DIFF(
            data_do_sms_ofi,
            data_de_entrada,
            DAY
            )
        END AS dias_atendimento,

        -- Dias para vencer
        date_diff(
            date_add(data_de_entrada, interval prazo_dias day),
            current_date(),
            day
        ) as dias_para_vencer,

        -- Status de prazo
        case
        WHEN data_de_entrada IS NULL OR prazo_dias IS NULL
            THEN 'Ignorado'

        WHEN data_do_sms_ofi IS NULL
            AND DATE_ADD(data_de_entrada, INTERVAL prazo_dias DAY) >= CURRENT_DATE()
            THEN 'Dentro do prazo'

        WHEN data_do_sms_ofi IS NULL
            AND DATE_ADD(data_de_entrada, INTERVAL prazo_dias DAY) < CURRENT_DATE()
            THEN 'Fora do prazo'

        WHEN data_do_sms_ofi <= DATE_ADD(data_de_entrada, INTERVAL prazo_dias DAY)
            THEN 'Dentro do prazo'

        ELSE 'Fora do prazo'
        END AS status_prazo

    from base
)

select * from tratado
