{{
    config(
        alias="vacinacao",
        schema="registro_vacinal",
        materialized="table",
        tags=['daily', 'vacinacao'],
        partition_by={
            "field": "particao_registro_vacinacao",
            "data_type": "date",
            "granularity": "month"
        },
    )
}}

with
    vacinacoes as (
        select *, 'historico' as origem
        from {{ ref("int_vacinacao__vitacare_historico") }}
        union all
        select *, 'api' as origem
        from {{ ref("int_vacinacao__vitacare_api") }}
        union all
        select *, 'continuo' as origem
        from {{ ref("int_vacinacao__vitacare_continuo") }}
        union all
        select *, 'sipni' as origem
        from {{ ref("int_vacinacao__sipni") }}
    ),

    vacinacoes_dedup as (
        select *
        from vacinacoes
        qualify row_number() over (
            partition by id_vacinacao 
            order by 
                case 
                    when origem = 'historico' then 1 
                    when origem = 'sipni' then 2 
                    when origem = 'api' then 3 
                    else 4 
                end
        ) = 1
    ),

    final as (
        select
            id_vacinacao,
            id_cnes,
            id_equipe,
            id_ine_equipe,
            id_microarea,
            estabelecimento_nome,
            vacina_nome,
            vacina_codigo,
            vacina_dose,
            vacina_lote,
            vacina_aplicacao_data,
            vacina_registro_data,
            vacina_registro_tipo,
            paciente_cns,
            paciente_cpf,
            paciente_nome,
            paciente_nascimento_data,
            paciente_nome_mae,
            paciente_obito,
            profissional_nome,
            profissional_cbo,
            profissional_cns,
            profissional_cpf,
            loaded_at,
            updated_at,
            case 
                when origem in ('historico', 'api', 'continuo') then 'vitacare'
                else origem
            end as origem,
            vacina_registro_data as particao_registro_vacinacao
        from vacinacoes_dedup
    )

select *
from final
