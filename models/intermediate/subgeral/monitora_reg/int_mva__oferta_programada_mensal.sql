-- combina os modelos de contagens a nivel de procedimentos, com os totais a nivel de
-- estabelecimentos, com os parametros dos procedimentos
select
    -- identificadores
    por_proced.cpf,
    por_proced.nome,
    por_proced.id_cnes,
    por_proced.estabelecimento_nome,
    por_proced.id_procedimento,
    por_proced.id_cbo_2002,
    por_proced.ocupacao,
    por_proced.ocupacao_familia,
    por_proced.id_cbo_2002_qtd_sisreg,
    por_proced.id_cbo_2002_todos_sisreg,

    por_proced.ano as ano_competencia,
    por_proced.mes as mes_competencia,

    -- contagens de vagas a nivel de procedimentos
    por_proced.vagas_programadas_mensal_primeira_vez,
    por_proced.vagas_programadas_mensal_retorno,
    por_proced.vagas_programadas_mensal_todas,

    -- contagens de vagas a nivel de unidades
    por_unidade.vagas_programadas_mensal_primeira_vez_unidade,
    por_unidade.vagas_programadas_mensal_retorno_unidade,
    por_unidade.vagas_programadas_mensal_todas_unidade,

    -- proporcao de vagas para cada procedimento em relação ao total de vagas na
    -- unidade (definido pelo proprio profissional)
    case
        when por_unidade.vagas_programadas_mensal_todas_unidade = 0
        then 0
        else
            round(
                por_proced.vagas_programadas_mensal_todas
                / por_unidade.vagas_programadas_mensal_todas_unidade,
                3
            )
    end as procedimento_distribuicao,

    -- parametros dos procedimentos
    padr_proced.descricao as procedimento,
    padr_proced.parametro_consultas_por_hora as procedimento_consultas_hora,
    round(
        padr_proced.parametro_reservas
        / (padr_proced.parametro_reservas + padr_proced.parametro_retornos),
        2
    ) as procedimento_proporcao_reservas,
    round(
        padr_proced.parametro_retornos
        / (padr_proced.parametro_reservas + padr_proced.parametro_retornos),
        2
    ) as procedimento_proporcao_retornos

from {{ ref("int_mva__oferta_programada_mensal_por_procedimento") }} as por_proced

left join
    {{ ref("int_mva__oferta_programada_mensal_por_unidade") }} as por_unidade using (
        cpf, id_cnes, ano, mes
    )

left join
    {{ ref("raw_sheets__assistencial_procedimento") }} as padr_proced using (
        id_procedimento
    )

where por_proced.cpf is not null
