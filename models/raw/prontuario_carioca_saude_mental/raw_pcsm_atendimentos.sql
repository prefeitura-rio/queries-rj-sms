{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="atendimentos",
        materialized="table",
        tags=["raw", "pcsm", "atendimentos", "daily"],
        description="Atendimentos simplificados realizados pela Prefeitura do Rio de Janeiro. O atendimento simplificado é um tipo de atendimento, não se encaixando em ambulatorial etc."
    )
}}

select
    safe_cast(seqatend as int64) as id_atendimento,
    {{
        dbt_utils.generate_surrogate_key(
                [
                    "seqatend",
                    "seqpac",
                    "sequs"
                ]
            )
        }} as id_hci,
    safe_cast(dtentrada as date) as data_entrada_atendimento,
    safe_cast(horaent as string) as hora_entrada_atendimento,
    safe_cast(dtsaida as date) as data_saida_atendimento,
    safe_cast({{ process_null('horasai') }} as string) as hora_saida_atendimento,
    safe_cast(seqtpatend as int64) as id_tipo_atendimento,
    safe_cast(codclin as string) as codigo_clinica,
    safe_cast(seqprof as int64) as id_profissional_saude,
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(seqativgrp as int64) as id_atividade_grupo,
    safe_cast(seqenatend as int64) as id_encaminhamento,
    safe_cast(sequsenc as int64) as id_unidade_saude_encaminhada,
    safe_cast(datcadast as date) as data_inclusao_cadastro,
    safe_cast({{ process_null('indlocalatend') }}  as string) as local_atendimento,
    case trim(safe_cast(indlocalatend as string))
        when 'C' then 'Atendimento na Unidade (CAPS)'
        when 'T' then 'Atendimento no território'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_local_atendimento,
    safe_cast({{ process_null('indatendcanc') }} as string) as atendimento_cancelado,
    case trim(safe_cast(indatendcanc as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_atendimento_cancelado,
    safe_cast({{ process_null('dsclstprof') }} as string) as lista_profissionais_atendimento,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_atendimentos') }}