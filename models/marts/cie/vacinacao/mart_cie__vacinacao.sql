{{
    config(
        alias="vacinacao",
        materialized="table",
        partition_by={
            "field": "particao_data_vacinacao",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

with
    vacinacoes as (
        select *, 'historico' as origem
        from {{ ref("int_historico_clinico__vacinacao__historico") }}
        union all
        select *, 'api' as origem
        from {{ ref("int_historico_clinico__vacinacao__api") }}
        union all
        select *, 'continuo' as origem
        from {{ ref("int_historico_clinico__vacinacao__continuo") }}
    ),

    vacinacoes_dedup as (
        select *
        from vacinacoes
        qualify row_number() over (
            partition by id_vacinacao 
            order by 
                case 
                    when origem = 'api' then 1 
                    when origem = 'historico' then 2 
                    else 3 
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
            paciente_id_prontuario,
            paciente_cns,
            paciente_cpf,
            estabelecimento_nome,
            equipe_nome,
            profissional_nome,
            profissional_cbo,
            profissional_cns,
            profissional_cpf,
            vacina_descricao,
            vacina_dose,
            vacina_lote,
            vacina_registro_tipo,
            vacina_estrategia,
            vacina_diff,
            vacina_aplicacao_data,
            vacina_registro_data,
            paciente_nome,
            paciente_sexo,
            paciente_nascimento_data,
            paciente_nome_mae,
            paciente_mae_nascimento_data,
            paciente_situacao,
            paciente_cadastro_data,
            paciente_obito,
            loaded_at,
            origem,
            safe_cast(paciente_cpf as int64) as cpf_particao
        from vacinacoes_dedup
    )

select *
from final

