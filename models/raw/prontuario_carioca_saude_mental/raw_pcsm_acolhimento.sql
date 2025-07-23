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
    safe_cast({{ process_null('horasai') }} as string) as hora_saida_acolhimento,
    safe_cast(seqprof as int64) as id_profissional,
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(seqprof2 as int64) as id_profissional_secundario,
    safe_cast(seqlogincad as int64) as id_funcionario_cadastramento,
    safe_cast(seqtpsaida as int64) as id_tipo_saida,
    safe_cast(indocupacao as string) as leito_ocupado,
    case trim(safe_cast(indocupacao as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as leito_ocupado_descricao,
    safe_cast(indleitoextra as string) as leito_extra,
    case trim(safe_cast(indleitoextra as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_leito_extra,
    safe_cast(indturno as string) as turno_acolhimento,
    case trim(safe_cast(indturno as string))
        when 'D' then 'Diurno'
        when '3' then 'Terceiro turno'
        when 'N' then 'Noturno'
        when 'D3N' then 'Diurno, Terceiro turno e Noturno'
        when 'D3' then 'Diurno e Terceiro turno'
        when 'DN' then 'Diurno e Noturno'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_turno_acolhimento,
    safe_cast(datcadast as date) as data_cadastro,
    safe_cast(indtipoleito as string) as tipo_leito,
    case trim(safe_cast(indtipoleito as string))
        when 'C' then 'Leito clínico'
        when 'A' then 'Leito de acolhimento'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_leito,    
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_acolhimento') }}