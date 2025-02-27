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
            coalesce(
                estab.nome_fantasia, ofer.estabelecimento_nome, prof.estabelecimento
            ) as estabelecimento,

            -- variaveis temporais
            coalesce(ofer.ano_competencia, prof.ano_competencia) as ano_competencia,
            coalesce(ofer.mes_competencia, prof.mes_competencia) as mes_competencia,

            -- procedimentos
            ofer.id_procedimento,
            {{ remove_accents_upper("ofer.procedimento") }} as procedimento,

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
            coalesce(
                prof.endereco_latitude, estab.endereco_latitude
            ) as endereco_latitude,
            coalesce(
                prof.endereco_longitude, estab.endereco_longitude
            ) as endereco_longitude,

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
            -- ma prática temporária (convertendo o tipo durante o join)
            on safe_cast(coalesce(ofer.ano_competencia, prof.ano_competencia) as int)
            = safe_cast(estab.ano_competencia as int)
            and safe_cast(coalesce(ofer.mes_competencia, prof.mes_competencia) as int)
            = safe_cast(estab.mes_competencia as int)
            and safe_cast(coalesce(ofer.id_cnes, prof.id_cnes) as int)
            = safe_cast(estab.id_cnes as int)
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

    iqr_label as (
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

        left join
            estabelecimentos as estab
            on safe_cast(mva.ano_competencia as int)
            = safe_cast(estab.ano_competencia as int)
            and safe_cast(mva.mes_competencia as int)
            = safe_cast(estab.mes_competencia as int)
            and safe_cast(mva.id_cnes as int) = safe_cast(estab.id_cnes as int)

        where estab.vinculo_sus_indicador = 1
    ),

    classifica_proceds as (
        select
            *,
            case
                when
                    procedimento like "%INFAN%"
                    or procedimento like "%PEDI%"
                    or procedimento like "%CRIAN%"
                    or procedimento like "%TESTE DA ORELHINHA%"
                    or procedimento
                    = "CONSULTA EM OFTALMOLOGIA - REFLEXO VERMELHO ALTERADO"
                    or procedimento = "CONSULTA ENDOCRINOLOGIA - CRESCIMENTO"
                    or procedimento = "CONSULTA ENDOCRINOLOGIA - CRESCIMENTO - PPI"
                    or procedimento = "POTENCIAL EVOCADO AUDITIVO BERA"
                    or procedimento = "GRUPO - INFECCOES CONGENITAS"
                then "CRIANCA"

                when procedimento like "%ADOLE%"
                then "ADOLESCENTE"

                else "ADULTO"
            end as procedimento_faixa_etaria
        from iqr_label
    ),

    final as (
        select
            cpf,
            cns,
            {{ remove_accents_upper("profissional") }} as profissional,
            id_cbo_2002,
            {{ remove_accents_upper("ocupacao") }} as ocupacao,
            {{ remove_accents_upper("ocupacao_agg") }} as ocupacao_agg,
            id_cbo_2002_qtd_sisreg,
            id_cbo_2002_todos_sisreg,
            id_cnes,
            {{ remove_accents_upper("estabelecimento") }} as estabelecimento,
            ano_competencia,
            mes_competencia,
            id_procedimento,
            procedimento,
            procedimento_faixa_etaria,
            carga_horaria_ambulatorial_mensal,
            carga_horaria_procedimento_esperada_mensal,
            vagas_programadas_mensal_todas,
            vagas_programadas_mensal_primeira_vez,
            vagas_programadas_mensal_retorno,
            vagas_esperadas_mensal,
            vagas_esperadas_mensal_primeira_vez,
            vagas_esperadas_mensal_retorno,
            vagas_diferenca_ofertado_esperado,
            procedimento_distribuicao,
            procedimento_consultas_hora,
            procedimento_proporcao_reservas,
            procedimento_proporcao_retornos,
            {{ remove_accents_upper("tipo_estabelecimento") }} as tipo_estabelecimento,
            {{ remove_accents_upper("tipo_estabelecimento_agrupado") }}
            as tipo_estabelecimento_agrupado,
            {{ remove_accents_upper("turno_estabelecimento") }}
            as turno_estabelecimento,
            {{ remove_accents_upper("tipo_gestao_estabelecimento") }}
            as tipo_gestao_estabelecimento,
            {{ remove_accents_upper("esfera_estabelecimento") }}
            as esfera_estabelecimento,
            {{ remove_accents_upper("natureza_juridica_estabelecimento") }}
            as natureza_juridica_estabelecimento,
            id_ap_estabelecimento,
            {{ remove_accents_upper("ap_estabelecimento") }} as ap_estabelecimento,
            {{ remove_accents_upper("endereco_bairro_estabelecimento") }}
            as endereco_bairro_estabelecimento,
            endereco_latitude,
            endereco_longitude,
            procedimento_ppi,
            sisreg_dados,
            cnes_dados,
            {{ remove_accents_upper("status_oferta") }} as status_oferta
        from classifica_proceds
    )

select *
from final
