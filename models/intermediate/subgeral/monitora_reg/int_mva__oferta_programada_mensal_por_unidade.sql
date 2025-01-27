-- vagas programadas pelos profissionais por unidade
-- view
select
    -- identificadores
    cpf,
    id_cnes,
    ano,
    mes,

    -- contagens de vagas
    sum(
        vagas_programadas_mensal_primeira_vez
    ) as vagas_programadas_mensal_primeira_vez_unidade,
    sum(vagas_programadas_mensal_retorno) as vagas_programadas_mensal_retorno_unidade,
    sum(vagas_programadas_mensal_todas) as vagas_programadas_mensal_todas_unidade

from {{ ref("int_mva__oferta_programada_mensal_por_procedimento") }}

group by cpf, id_cnes, ano, mes
