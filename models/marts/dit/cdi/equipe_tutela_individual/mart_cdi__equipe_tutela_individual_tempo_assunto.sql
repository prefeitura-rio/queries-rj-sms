{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'equipe_tutela_individual_tempo_atendimento'
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
        case
            when (
                regexp_contains(upper(sexo), r'M')
                and regexp_contains(upper(sexo), r'F')
            )
            or upper(trim(sexo)) = 'AMBOS'
                then 'Nucleo Familiar'

            -- Núcleo familiar (NF explícito)
            when upper(trim(sexo)) = 'NF'
                then 'Nucleo Familiar'

            -- Feminino
            when upper(trim(sexo)) = 'F'
                then 'Feminino'

            -- Masculino
            when upper(trim(sexo)) = 'M'
                then 'Masculino'

            -- Qualquer outro valor
            else 'Não identificado'
        end as sexo_tratado,
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
            when situacao = "RESOLVIDO" then date_diff(
                data_do_sms_ofi,
                data_de_entrada,
                day
            )
            else null
        end as tempo_atendimento_dias,


        case
            when situacao <> "RESOLVIDO"
                then 'Resolvido'

            when data_do_sms_ofi
                 <= date_add(data_de_entrada, interval prazo_dias day)
                then 'Dentro do prazo'

            else 'Fora do prazo'
        end as status_prazo

    from base
)

select *
from calculos
