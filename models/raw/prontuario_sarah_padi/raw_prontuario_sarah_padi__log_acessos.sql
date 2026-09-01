{{
    config(
        alias="log_acessos",
        materialized="incremental",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        schema='brutos_prontuario_sarah_padi',
    )
}}


{% set last_partition = get_last_partition_date(this) %}

with source as (
    select
        *
    from {{ source("brutos_prontuario_sarah_padi_staging", "log_acessos") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('data') }} as data,
        {{ base64_to_string('hora') }} as hora,
        {{ base64_to_string('evento') }} as evento,
        {{ base64_to_string('usuario') }} as usuario,
        {{ base64_to_string('cargo') }} as cargo,
        {{ base64_to_string('data_inicio_agenda') }} as data_inicio_agenda,
        {{ base64_to_string('hora_inicio_agenda') }} as hora_inicio_agenda,
        {{ base64_to_string('quantidade') }} as quantidade,
        extracted_at,
        ano_particao,
        mes_particao,
        data_particao
    from source
),

renomeado as (
    select
        safe_cast(data as date) as data,
        safe_cast(hora as time) as hora,
        safe_cast(evento as string) as evento,
        safe_cast(usuario as string) as usuario,
        safe_cast(cargo as string) as cargo,
        safe_cast(data_inicio_agenda as date) as agenda_data_inicio,
        safe_cast(hora_inicio_agenda as time) as agenda_hora_inicio,
        safe_cast(quantidade as int64) as quantidade,
        
        -- Metadados
        safe_cast(extracted_at as datetime) as extracted_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from base64_para_string
    qualify row_number() over (partition by data, hora, evento, usuario order by extracted_at desc) = 1
)

select * from renomeado