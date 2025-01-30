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

    estabelecimentos as (
        select *
        from {{ ref("dim_estabelecimento_sus_rio_historico") }}
        where
            data_particao = (
                select max(data_particao)
                from {{ ref("dim_estabelecimento_sus_rio_historico") }}
            )
    ),

    mva as (
        select
            -- identificadores
            coalesce(ofer.cpf, prof.cpf) as cpf,
            prof.cns,
            coalesce(prof.profissional, ofer.nome) as profissional,
            coalesce(prof.id_cbo_2002, ofer.id_cbo_2002) as id_cbo_2002,
            ofer.id_cbo_2002_qtd_sisreg,
            ofer.id_cbo_2002_todos_sisreg,
            coalesce(prof.ocupacao, ofer.ocupacao) as ocupacao,
            coalesce(prof.ocupacao_agg, ofer.ocupacao_familia) as ocupacao_agg,
            coalesce(ofer.id_cnes, prof.id_cnes) as id_cnes,
            coalesce(estab.nome_fantasia, ofer.estabelecimento_nome) as estabelecimento,

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
                        * ofer.procedimento_distribuicao,
                        3
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
            coalesce(prof.esfera, estab.esfera) as esfera_estabelecimento,
            coalesce(
                prof.natureza_juridica, estab.natureza_juridica_descr
            ) as natureza_juridica_estabelecimento,
            coalesce(
                prof.tipo_gestao, estab.tipo_gestao_descr
            ) as tipo_gestao_estabelecimento,
            coalesce(prof.turno, estab.turno_atendimento) as turno_estabelecimento,
            coalesce(
                prof.tipo_unidade_alternativo, estab.tipo_unidade_alternativo
            ) as tipo_estabelecimento,
            coalesce(
                prof.tipo_unidade_agrupado, estab.tipo_unidade_agrupado
            ) as tipo_estabelecimento_agrupado,
            coalesce(prof.id_ap, estab.id_ap) as id_ap_estabelecimento,
            coalesce(prof.ap, estab.ap) as ap_estabelecimento,
            coalesce(
                prof.endereco_bairro, estab.endereco_bairro
            ) as endereco_bairro_estabelecimento,

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

        left join
            estabelecimentos as estab
            on ofer.ano_competencia = estab.ano_competencia
            and ofer.mes_competencia = estab.mes_competencia
            and ofer.id_cnes = estab.id_cnes
    ),

    iqr as (
        select
            ano_competencia,
            mes_competencia,
            id_procedimento,
            approx_quantiles(vagas_diferenca_ofertado_esperado, 4)[offset(1)] as q1,
            approx_quantiles(vagas_diferenca_ofertado_esperado, 4)[offset(3)] as q3
        from mva
        group by ano_competencia, mes_competencia, id_procedimento
    ),

    final as (
        select
            mva.*,

            case
                when mva.vagas_diferenca_ofertado_esperado is null
                then 'N/A'
                when mva.vagas_diferenca_ofertado_esperado < q1 - 1.5 * (q3 - q1)
                then 'MUITO BAIXA'
                when mva.vagas_diferenca_ofertado_esperado < q1
                then 'BAIXA'
                when mva.vagas_diferenca_ofertado_esperado <= q3
                then 'ADEQUADA'
                else 'ALTA'
            end as status_oferta

        from mva
        left join
            iqr
            on mva.ano_competencia = iqr.ano_competencia
            and mva.mes_competencia = iqr.mes_competencia
            and mva.id_procedimento = iqr.id_procedimento
        where mva.estabelecimento is not null  -- removendo registros do CNES de estabelecimentos sem vinculo com o SUS

    )

select *
from final
