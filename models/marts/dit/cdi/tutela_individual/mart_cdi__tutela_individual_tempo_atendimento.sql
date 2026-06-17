{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'equipe_tutela_individual_tempo_atendimento',
        meta={"owner": "karen"}
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__equipe_tutela_individual') }}

),

calculos as (

    select
        processo_rio,

        orgao,
        case
            when (
                regexp_contains(upper(sexo), r'M')
                and regexp_contains(upper(sexo), r'F')
            )
            or upper(trim(sexo)) = 'AMBOS'
                then 'Nucleo Familiar'

            when upper(trim(sexo)) = 'NF'
                then 'Nucleo Familiar'

            when upper(trim(sexo)) = 'F'
                then 'Feminino'

            when upper(trim(sexo)) = 'M'
                then 'Masculino'

            else 'Não identificado'
        end as sexo_tratado,
        idade_categoria,
        area,
        promotor_defensor,

        assuntos,

        data_entrada,
        data_finalizacao,

        case
            when situacao = "RESOLVIDO" then date_diff(
                data_finalizacao,
                data_entrada,
                day
            )
            else null
        end as tempo_atendimento_dias,

        case
            when situacao = "RESOLVIDO"
                then 'Resolvido'

            when data_finalizacao
                 <= date_add(data_entrada, interval prazo_dias day)
                then 'Dentro do prazo'

            else 'Fora do prazo'
        end as status_prazo

    from base
)

select *
from calculos