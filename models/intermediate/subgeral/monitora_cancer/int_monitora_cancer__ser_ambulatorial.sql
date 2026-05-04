-- Eventos de procedimentos de mama extraídos do SER (regulação estadual ambulatorial)
{% set data_inicio_monitoramento = "2021-01-01" %}

with
    procedimentos as (
        select
            id_procedimento,
            procedimento,
            safe_cast(criterio_suspeita as bool) as criterio_suspeita,
            safe_cast(criterio_diagnostico as bool) as criterio_diagnostico
        from {{ ref("procedimentos_ser") }}
    ),

    base as (
        select
            *,
            safe_cast(cod_recurso_solicitado as int) as id_procedimento_solicitado,
            safe_cast(cod_recurso_regulado as int) as id_procedimento_regulado
        from {{ ref("raw_ser_metabase__ambulatorial") }}
        where data_solicitacao >= "{{ data_inicio_monitoramento }}"
    ),

    enriquecido as (
        select
            base.*,
            ps.procedimento as procedimento_solicitado_seed,
            ps.criterio_suspeita as suspeita_solicitado,
            ps.criterio_diagnostico as diagnostico_solicitado,
            pr.procedimento as procedimento_regulado_seed,
            pr.criterio_suspeita as suspeita_regulado,
            pr.criterio_diagnostico as diagnostico_regulado
        from base
            left join procedimentos as ps
                on base.id_procedimento_solicitado = ps.id_procedimento
            left join procedimentos as pr
                on base.id_procedimento_regulado = pr.id_procedimento
        where ps.id_procedimento is not null
            or pr.id_procedimento is not null
    )

select
-- pk
    "SER" as sistema_origem,
    "REGULACAO" as sistema_tipo,
    id_solicitacao as id_sistema_origem,

-- paciente
    paciente_cns,
    cast(NULL as int) as paciente_cpf_sisreg,

-- unidades
    id_cnes_unidade_origem,
    id_cnes_unidade_executante,

-- qualificacao
    cid,
    solicitacao_estado as evento_status,
    coalesce(procedimento_regulado_seed, procedimento_solicitado_seed) as procedimento,

-- datas
    data_solicitacao,
    data_agendamento as data_autorizacao,
    data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    coalesce(suspeita_solicitado, false)
        or coalesce(suspeita_regulado, false) as criterio_suspeita,
    coalesce(diagnostico_solicitado, false)
        or coalesce(diagnostico_regulado, false) as criterio_diagnostico

from enriquecido
