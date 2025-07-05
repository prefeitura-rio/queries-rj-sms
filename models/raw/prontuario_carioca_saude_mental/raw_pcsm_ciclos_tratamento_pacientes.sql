{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="ciclos_tratamento_pacientes",
        materialized="table",
        tags=["raw", "pcsm", "ciclos", "tratamento", "pacientes"],
        description="Ciclos de pacientes de caps da Prefeitura do Rio de Janeiro. Ciclo é um conjunto de atendimentos feitos em um ambulatório ou em um caps. Não se pode ter mais de um ciclo aberto ao mesmo tempo nem em um ambulatório nem em um CAPS."
    )
}}

select
    safe_cast(seqciclo as int64) as id_ciclo,
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(indciclo as string) as tipo_ciclo,
    safe_cast(dtentrada as date) as data_inicio_ciclo,
    safe_cast(horentrada as string) as hora_inicio_ciclo,
    safe_cast(dtsaida as date) as data_termino_ciclo,
    safe_cast(horsaida as string) as hora_termino_ciclo,
    safe_cast(indmotivsaida as string) as fechamento_ciclo,
    safe_cast(seqpacorig as int64) as id_paciente_unificado
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_pac_ciclos') }}