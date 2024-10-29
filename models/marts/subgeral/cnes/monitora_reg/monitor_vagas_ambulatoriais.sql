-- calcula indicadores a partir da união dos dados de oferta programada com os dados
-- de vinculação dos profissionais
{{ config(schema="projeto_monitora_reg", alias="monitor_vagas_ambulatoriais") }}

with
    int_carga_horaria_ambulatorial_mensal as (
        select * from {{ ref("int_mva__carga_horaria_ambulatorial_mensal") }}
    ),

    max_ano_mes as (
        select ano_competencia as ano_max, mes_competencia as mes_max
        from int_carga_horaria_ambulatorial_mensal
        order by ano_competencia desc, mes_competencia desc
        limit 1
    ),

    int_oferta_programada_mensal as (
        select *
        from {{ ref("int_mva__oferta_programada_mensal") }}
        where
            (ano_competencia < (select ano_max from max_ano_mes))
            or (
                ano_competencia = (select ano_max from max_ano_mes)
                and mes_competencia <= (select mes_max from max_ano_mes)
            )
    ),

    mva as (
        select
            -- identificadores
            coalesce(ofer.cpf, prof.cpf) as cpf,
            prof.cns,
            prof.profissional,
            coalesce(ofer.id_cbo_2002, prof.id_cbo_2002) as id_cbo_2002,
            prof.ocupacao,
            prof.ocupacao_agg,
            coalesce(ofer.id_cnes, prof.id_cnes) as id_cnes,
            prof.estabelecimento,

            -- variaveis temporais
            coalesce(ofer.ano_competencia, prof.ano_competencia) as ano_competencia,
            coalesce(ofer.mes_competencia, prof.mes_competencia) as mes_competencia,

            -- procedimentos
            ofer.id_procedimento,
            ofer.procedimento,

            -- cargas horarias e vagas
            prof.carga_horaria_ambulatorial_mensal,
            case
                when
                    ofer.vagas_programadas_mensal_todas_unidade is not null
                    and prof.carga_horaria_ambulatorial_mensal is null
                then null
                when
                    ofer.vagas_programadas_mensal_todas_unidade is null
                    and prof.carga_horaria_ambulatorial_mensal is not null
                then prof.carga_horaria_ambulatorial_mensal
                else
                    round(
                        prof.carga_horaria_ambulatorial_mensal
                        * ofer.procedimento_distribuicao
                    )
            end as carga_horaria_procedimento_esperada_mensal,

            -- vagas oferecidas
            ofer.vagas_programadas_mensal_todas,
            ofer.vagas_programadas_mensal_primeira_vez,
            ofer.vagas_programadas_mensal_retorno,

            -- vagas esperadas
            case
                when
                    ofer.vagas_programadas_mensal_todas_unidade is not null
                    and prof.carga_horaria_ambulatorial_mensal is null
                then null
                else
                    round(
                        prof.carga_horaria_ambulatorial_mensal
                        * ofer.procedimento_distribuicao
                        * ofer.procedimento_consultas_hora
                    )
            end as vagas_esperadas_mensal,

            case
                when
                    ofer.vagas_programadas_mensal_todas_unidade is not null
                    and prof.carga_horaria_ambulatorial_mensal is null
                then null
                else
                    round(
                        prof.carga_horaria_ambulatorial_mensal
                        * ofer.procedimento_distribuicao
                        * ofer.procedimento_consultas_hora
                        * ofer.procedimento_proporcao_reservas
                    )
            end as vagas_esperadas_mensal_primeira_vez,

            case
                when
                    ofer.vagas_programadas_mensal_todas_unidade is not null
                    and prof.carga_horaria_ambulatorial_mensal is null
                then null
                else
                    round(
                        prof.carga_horaria_ambulatorial_mensal
                        * ofer.procedimento_distribuicao
                        * ofer.procedimento_consultas_hora
                        * ofer.procedimento_proporcao_retornos
                    )
            end as vagas_esperadas_mensal_retorno,

            -- diferenca entre a quantidade de vagas esperadas e ofertadas
            case
                when prof.carga_horaria_ambulatorial_mensal = 0
                then null
                else
                    (
                        round(ofer.vagas_programadas_mensal_todas) - round(
                            prof.carga_horaria_ambulatorial_mensal
                            * ofer.procedimento_distribuicao
                            * ofer.procedimento_consultas_hora
                        )
                    )
            end as vagas_diferenca_ofertado_esperado,

            -- parametros dos procedimentos
            ofer.procedimento_distribuicao,
            ofer.procedimento_consultas_hora,
            ofer.procedimento_proporcao_reservas,
            ofer.procedimento_proporcao_retornos,

            -- informacoes das unidades
            prof.esfera as esfera_estabelecimento,
            prof.natureza_juridica as natureza_juridica_estabelecimento,
            prof.tipo_gestao as tipo_gestao_estabelecimento,
            prof.turno as turno_estabelecimento,
            prof.tipo_unidade_alternativo as tipo_estabelecimento,
            prof.tipo_unidade_agrupado as tipo_estabelecimento_agrupado,
            prof.id_ap as id_ap_estabelecimento,
            prof.ap as ap_estabelecimento,
            prof.endereco_bairro as endereco_bairro_estabelecimento,

            -- flags
            case
                when lower(ofer.procedimento) like '%ppi%' then 1 else 0
            end as procedimento_ppi,

            case
                when ofer.vagas_programadas_mensal_todas_unidade is null then 0 else 1
            end as sisreg_dados,

            case
                when prof.carga_horaria_ambulatorial_mensal is null then 0 else 1
            end as cnes_dados

        from int_oferta_programada_mensal as ofer
        full outer join
            int_carga_horaria_ambulatorial_mensal as prof

            -- má prática temporária (convertendo o tipo durante o join)
            on safe_cast(ofer.cpf as int64) = safe_cast(prof.cpf as int64)
            and safe_cast(ofer.id_cnes as int64) = safe_cast(prof.id_cnes as int64)
            and safe_cast(ofer.ano_competencia as int64)
            = safe_cast(prof.ano_competencia as int64)
            and safe_cast(ofer.mes_competencia as int64)
            = safe_cast(prof.mes_competencia as int64)
            and safe_cast(ofer.id_cbo_2002 as int64)
            = safe_cast(prof.id_cbo_2002 as int64)
    )

select *
from mva
