-- Unpivot the columns of the table raw_seguir_em_frente__controle_presenca
with
    -- sources
    presenca as (
        select *
        from {{ ref("raw_seguir_em_frente__controle_presenca") }}
        where
            data_particao = (
                select max(data_particao)
                from {{ ref("raw_seguir_em_frente__controle_presenca") }}
            )
    ),

    -- transformations
    prensenca_pivoted as (
        {{
            dbt_utils.unpivot(
                ref("raw_seguir_em_frente__controle_presenca"),
                cast_to="string",
                exclude=[
                    "cpf",
                    "observacoes",
                    "anexos",
                    "criado_por",
                    "criado_em",
                    "periodo_cadastrado_tipo",
                    "periodo_cadastrado_dia",
                    "periodo_cadastrado_semana",
                ],
                remove=[
                    "id",
                    "id_nome_cpf",
                    "data_particao",
                    "ano_particao",
                    "mes_particao",
                ],
                field_name="registro_dia",
                value_name="registro_valor",
            )
        }}
    ),

    presenca_valid_records as (
        select * from prensenca_pivoted where registro_valor != ""
    ),

    final as (
        select
            *,
            case
                when contains_substr(registro_dia, "segunda")
                then date_add(periodo_cadastrado_data_inicial, interval 1 day)
                when contains_substr(registro_dia, "terca")
                then date_add(periodo_cadastrado_data_inicial, interval 2 day)
                when contains_substr(registro_dia, "quarta")
                then date_add(periodo_cadastrado_data_inicial, interval 3 day)
                when contains_substr(registro_dia, "quinta")
                then date_add(periodo_cadastrado_data_inicial, interval 4 day)
                when contains_substr(registro_dia, "sexta")
                then date_add(periodo_cadastrado_data_inicial, interval 5 day)
                else safe_cast(periodo_cadastrado_dia as date)
            end as registro_data
        from
            (
                select
                    *,
                    if(
                        periodo_cadastrado_dia != "",
                        safe_cast(periodo_cadastrado_dia as date),
                        parse_date('%d/%m/%y', substr(periodo_cadastrado_semana, 1, 8))
                    ) as periodo_cadastrado_data_inicial,
                from presenca_valid_records
                order by cpf, periodo_cadastrado_dia, periodo_cadastrado_semana
            )
    )

select
    -- sha256(concat(cpf, registro_data)) as id,
    cpf,
    observacoes,
    anexos,
    criado_por,
    criado_em,
    periodo_cadastrado_tipo,
    periodo_cadastrado_dia,
    periodo_cadastrado_semana,
    -- registro_dia,
    safe_cast(registro_data as date) as registro_data,
    registro_valor
from final
order by cpf, registro_data asc
