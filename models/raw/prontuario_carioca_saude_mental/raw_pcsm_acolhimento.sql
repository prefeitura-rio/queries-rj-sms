{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="acolhimentos",
        materialized="table",
        tags=["raw", "pcsm", "acolhimento"],
        description="Acolhimentos feitos em unidades de acolhimento (tipos especiais de unidades de saúde) da Prefeitura do Rio de Janeiro. Acolhimento é a recepção temporária para o cuidado de pacientes de saúde mental. Um acolhimento é período de uso de um leito."
    )
}}

select
    safe_cast(seqacolhe as int64) as id_acolhimento,
    safe_cast(dtentrada as date) as data_entrada_acolhimento,
    safe_cast(horaent as string) as hora_entrada_acolhimento,
    safe_cast(dtsaida as date) as data_saida_acolhimento,
    safe_cast(horasai as string) as hora_saida_acolhimento,
    safe_cast(seqprof as int64) as id_profissional,
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(seqprof2 as int64) as id_profissional_secundario,
    safe_cast(seqlogincad as int64) as id_funcionario_cadastramento,
    safe_cast(seqtpsaida as int64) as id_tipo_saida,
    safe_cast(indocupacao as string) as leito_ocupado,
    safe_cast(indleitoextra as string) as leito_extra,
    safe_cast(indturno as string) as turno_acolhimento,
    safe_cast(datcadast as date) as data_cadastro,
    safe_cast(indtipoleito as string) as tipo_leito
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_acolhimento') }}