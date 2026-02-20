{{ 
    config(
        materialized = 'table',
        schema = 'marts_cdi',
        alias = 'mart_cdi__equipe_tutela_individual_v2_tempo_atendimento'
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__equipe_tutela_individual') }}

),

calculos as (

    select
        -- identificador
        processo_rio,

        -- dimensões para filtros
        orgao,
        sexo,
        classificacao_idade,
        area,
        promotora_defensora,

        -- assunto (tipo de solicitação)
        assuntos,

        -- datas base
        data_de_entrada,
        data_do_sms_ofi,

        -- tempo de atendimento (por processo)
        case
            when data_de_entrada is null
              or data_do_sms_ofi is null
                then null
            when data_do_sms_ofi < data_de_entrada
                then null
            else date_diff(
                data_do_sms_ofi,
                data_de_entrada,
                day
            )
        end as tempo_atendimento_dias,

        -- status de atendimento
        case
            when data_do_sms_ofi is null
                then 'Em aberto'
            else 'Finalizados'
        end as status_atendimento_finalizados,

        case
            when data_de_entrada is null
              or data_do_sms_ofi is null
              or prazo_dias is null
                then 'Ignorado'

            when data_do_sms_ofi
                 <= date_add(data_de_entrada, interval prazo_dias day)
                then 'Dentro do prazo'

            else 'Fora do prazo'
        end as status_prazo

    from base
    where processo_rio is not null
)

select *
from calculos
