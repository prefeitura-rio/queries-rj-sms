-- combina os modelos de contagens a nivel de procedimentos, com os totais a nivel de estabelecimentos, com os parametros dos procedimentos
-- view

select
    -- identificadores
    por_proced_cbo.cpf,
    por_proced_cbo.id_cnes,
    por_proced_cbo.id_procedimento,
    por_proced_cbo.id_cbo_2002,
    por_proced_cbo.ano,
    por_proced_cbo.mes,

    -- contagens de vagas a nivel de procedimentos
    por_proced_cbo.vagas_programadas_mensal_primeira_vez,
    por_proced_cbo.vagas_programadas_mensal_retorno,
    por_proced_cbo.vagas_programadas_mensal_todas,

    -- contagens de vagas a nivel de unidades
    por_unidade.vagas_programadas_mensal_primeira_vez_unidade,
    por_unidade.vagas_programadas_mensal_retorno_unidade,
    por_unidade.vagas_programadas_mensal_todas_unidade,

    -- proporcao de vagas para cada procedimento em relação ao total de vagas na unidade (definido pelo proprio profissional)
    case
        when por_unidade.vagas_programadas_mensal_todas_unidade = 0 then 0
        else round(por_proced_cbo.vagas_programadas_mensal_todas / por_unidade.vagas_programadas_mensal_todas_unidade, 3)
    end as procedimento_distribuicao,

    -- parametros dos procedimentos
    padr_proced.descricao as procedimento,
    padr_proced.parametro_consultas_por_hora as procedimento_consultas_hora,
    round(padr_proced.parametro_reservas / (padr_proced.parametro_reservas + padr_proced.parametro_retornos), 2) as procedimento_proporcao_reservas,
    round(padr_proced.parametro_retornos / (padr_proced.parametro_reservas + padr_proced.parametro_retornos), 2) as procedimento_proporcao_retornos

from {{ ref('int_mva__oferta_programada_mensal_por_procedimento_cbo') }} as por_proced_cbo

left join {{ ref('int_mva__oferta_programada_mensal_por_unidade') }} as por_unidade
using (cpf, id_cnes, ano, mes)

left join {{ ref("raw_sheets__assistencial_procedimento") }} as padr_proced
using (id_procedimento)