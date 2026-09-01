{{
    config(
        alias="prescricao_medica_itens",
        materialized="incremental",
        unique_key="id_atendimento",
        schema='brutos_prontuario_sarah_padi',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


{% set last_partition = get_last_partition_date(this) %}

with source as (
    select
        *
    from {{ source("brutos_prontuario_sarah_padi_staging", "prescricao_medica_itens") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('data_prescricao') }} as data_prescricao,
        {{ base64_to_string('prescricao') }} as prescricao,
        {{ base64_to_string('prestador_prescricao') }} as prestador_prescricao,
        {{ base64_to_string('paciente') }} as paciente,
        {{ base64_to_string('tipo') }} as tipo,
        {{ base64_to_string('item_id') }} as item_id,
        {{ base64_to_string('item_nome') }} as item_nome,
        {{ base64_to_string('item_quantidade') }} as item_quantidade,
        {{ base64_to_string('unidade') }} as unidade,
        {{ base64_to_string('via_administracao_nome') }} as via_administracao_nome,
        {{ base64_to_string('via_administracao_sigla') }} as via_administracao_sigla,
        {{ base64_to_string('posologia') }} as posologia,
        {{ base64_to_string('referencia') }} as referencia,
        {{ base64_to_string('inicio') }} as inicio,
        {{ base64_to_string('situacao') }} as situacao,
        {{ base64_to_string('data_aprazamento') }} as data_aprazamento,
        {{ base64_to_string('hora_aprazamento') }} as hora_aprazamento,
        {{ base64_to_string('prestador_checagem') }} as prestador_checagem,
        {{ base64_to_string('data_evento') }} as data_evento,
        {{ base64_to_string('hora_evento') }} as hora_evento,
        {{ base64_to_string('suspenso') }} as suspenso,
        {{ base64_to_string('quantidade') }} as quantidade,
        extracted_at,
        ano_particao,
        mes_particao,
        data_particao
    from source
),

renomeado as (
    select
        safe_cast(data_prescricao as date) as prescricao_data,
        safe_cast(prescricao as string) as prescricao,
        safe_cast(prestador_prescricao as string) as prestador,
        safe_cast(paciente as string) as paciente,
        safe_cast(tipo as string) as tipo,
        safe_cast(item_id as int64) as id_item,
        safe_cast(item_nome as string) as item_nome,
        safe_cast(item_quantidade as numeric) as item_quantidade,
        safe_cast(unidade as string) as unidade,
        safe_cast(via_administracao_nome as string) as via_administracao_nome,
        safe_cast(via_administracao_sigla as string) as via_administracao_sigla,
        safe_cast(posologia as string) as posologia,
        safe_cast(referencia as int64) as referencia,
        safe_cast(inicio as string) as inicio,
        safe_cast(situacao as string) as situacao,
        safe_cast(data_aprazamento as date) as data_aprazamento,
        safe_cast(hora_aprazamento as time) as hora_aprazamento,
        safe_cast(prestador_checagem as string) as prestador_checagem,
        safe_cast(data_evento as date) as data_evento,
        safe_cast(hora_evento as time) as hora_evento,
        safe_cast(suspenso as string) as suspenso,
        safe_cast(quantidade as int64) as quantidade,
        
        -- Metadados
        safe_cast(extracted_at as datetime) as extracted_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    
    from base64_para_string
)

select
    *

from renomeado