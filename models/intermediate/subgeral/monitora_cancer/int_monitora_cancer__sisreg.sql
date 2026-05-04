-- Eventos de procedimentos de mama extraídos do SISREG (regulação ambulatorial)
{% set data_inicio_monitoramento = "2021-01-01" %}

with
    procedimentos as (
        select
            id_procedimento,
            procedimento,
            safe_cast(criterio_suspeita as bool) as criterio_suspeita,
            safe_cast(criterio_diagnostico as bool) as criterio_diagnostico
        from {{ ref("procedimentos_sisreg") }}
    ),

    base as (
        select *
        from {{ ref("mart_sisreg__solicitacoes") }}
        where data_solicitacao >= "{{ data_inicio_monitoramento }}"
    )

select
-- pk
    "SISREG" as sistema_origem,
    "REGULACAO" as sistema_tipo,
    safe_cast(base.id_solicitacao as int) as id_sistema_origem,

-- paciente
    base.paciente_cns,
    safe_cast(base.paciente_cpf as int) as paciente_cpf_sisreg,

-- unidades
    base.id_cnes_unidade_solicitante as id_cnes_unidade_origem,
    base.id_cnes_unidade_executante,

-- qualificacao
    base.cid_solicitacao as cid,
    base.solicitacao_status as evento_status,
    proc.procedimento,

-- datas
    safe_cast(base.data_solicitacao as date) as data_solicitacao,
    safe_cast(base.data_autorizacao as date) as data_autorizacao,
    safe_cast(base.data_execucao as date) as data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    proc.criterio_suspeita as criterio_suspeita,
    proc.criterio_diagnostico as criterio_diagnostico

from base
    inner join procedimentos as proc
        on safe_cast(base.id_procedimento_sisreg as int) = proc.id_procedimento
