{{
    config(
        schema="saude_historico_clinico",
        alias="vacinacao",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
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
        select
            * except(vacina_descricao),

            -- Remove prefixos "vacina", "vacina contra", ...
            REGEXP_REPLACE(
                trim({{ process_null("vacina_descricao") }}),
                r"(?i)^vacina\s*(contra(\s*[oa])?)?\s*",
                ""
            ) as vacina_descricao

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

    nomes_vacinas as (
        select *
        from {{ ref("raw_sheets__vacinas_padronizadas") }}
    ),

    vacinacoes_padronizado as (
        select
            dedup.*,
            coalesce(
                nomes.nome_para,
                {{ proper_br("dedup.vacina_descricao") }}
             ) as vacina_descricao_padronizada,
             nomes.sigla as vacina_sigla,
             nomes.detalhes as vacina_detalhes

        from vacinacoes_dedup as dedup
        left join nomes_vacinas as nomes
            -- Aqui fazemos um pequeno tratamento para aumentar
            -- as chances de achar um par na planilha
            on nomes.nome_de = REGEXP_REPLACE(
                REGEXP_REPLACE(
                    dedup.vacina_descricao,
                    -- Remove parênteses, vírgulas
                    r"[,\(\)]",
                    ""
                ),
                -- Remove espaços em branco duplicados
                r"\s{2,}",
                " "
            )
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
            {{ proper_estabelecimento("estabelecimento_nome") }} as estabelecimento_nome,
            equipe_nome,
            profissional_nome,
            profissional_cbo,
            profissional_cns,
            profissional_cpf,
            vacina_descricao,
            vacina_descricao_padronizada,
            vacina_sigla,
            vacina_detalhes,

            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                {{ capitalize_first_letter("vacina_dose") }},
                                "eforco",  -- ex. "Reforco"
                                "eforço"
                            ),
                            "acinacao", -- ex. "Revacinacao"
                            "acinação"
                        ),
                        "unica", -- ex. "Dose unica"
                        "única"
                    ),
                    r"([0-9]+)\s*dose",  -- ex. "2 dose" -> "2ª dose"
                    r"\1ª dose"
                ),
                r"([0-9]+)\s*reforço",  -- ex. "2 reforço" -> "2º reforço"
                r"\1º reforço"
            ) as vacina_dose,

            {{ clean_lote_vacina("vacina_lote") }} as vacina_lote,

            vacina_registro_tipo,
            vacina_estrategia,
            vacina_diff,

            -- Diversos casos de data de aplicação 1900-01-01
            if(
                -- TODO: quando tivermos dados de 1900, atualizar
                vacina_aplicacao_data > date("1900-01-01"),
                vacina_aplicacao_data,
                null
            ) as vacina_aplicacao_data,
            if(
                vacina_registro_data > date("1900-01-01"),
                vacina_registro_data,
                null
            ) as vacina_registro_data,
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
        from vacinacoes_padronizado
    )

select *
from final
