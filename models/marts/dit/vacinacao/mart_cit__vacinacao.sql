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
            {{ dbt_utils.generate_surrogate_key([
                'id_cnes',
                'vacina_codigo',
                'vacina_registro_data',
                'vacina_dose',
                'coalesce(paciente_cns, paciente_cpf, paciente_nome, id_vacinacao)' 
                -- usando id_vacinacao como fallback caso n existe info de paciente,
                -- se for de fontes diferentes, mante, se for da mesma fonte e ter o mesmo id, vai ter o dedup
            ]) }} as id_vacinacao,
            id_vacinacao as id_vacinacao_fonte,
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

            -- Padronizacao do tipo de registro
            case
              when lower(trim(vacina_registro_tipo)) in (
                'administracao', 
                'administração', 
                'administrao', 
                'vaccine administration'
              ) then 'Administração'
              
              when lower(trim(vacina_registro_tipo)) in (
                'registro de aplicação anterior', 
                'registro de aplicacao anterior', 
                'registro de vacinação anterior', 
                'registro de vacinao anterior', 
                'registro anterior/transcrição de caderneta', 
                'register of a past vaccine administration (resgate)'
              ) then 'Registro de aplicação anterior'
              
              when lower(trim(vacina_registro_tipo)) in (
                'não aplicada', 
                'nao aplicada', 
                'no aplicada', 
                'nao aplicavel', 
                'non applicable'
              ) then 'Não aplicada'
              
              when lower(trim(vacina_registro_tipo)) in (
                'vacina no exterior'
              ) then 'Vacina no exterior'
              
              else initcap(vacina_registro_tipo)
            end as vacina_registro_tipo,
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
    qualify row_number() over (partition by id_vacinacao order by loaded_at desc, updated_at desc) = 1