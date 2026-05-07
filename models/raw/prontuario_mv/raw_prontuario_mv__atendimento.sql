{{
    config(
        alias="atendimento",
        materialized="incremental",
        schema='brutos_prontuario_mv',
        incremental_strategy="insert_overwrite",
        unique_key = "id_hci",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['mv']
    )
}}


with 

source as (
    select * 
    from {{ source("brutos_prontuario_mv_api_staging", "paciente_continuo") }}   
    {% if is_incremental() %}
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) >= TIMESTAMP(date_sub(current_date('America/Sao_Paulo'), interval 30 day))
    {% endif %}
),


paciente_json as (
    select 
        payload_cnes as id_cnes,
        json_extract_scalar(data, '$.numero_atendimento') as numero_atendimento,
        json_extract_scalar(data, '$.nome_paciente') as nome_paciente,
        json_extract_scalar(data, '$.cns_paciente') as cns_paciente,
        json_extract_scalar(data, '$.nome_social_paciente') as nome_social_paciente,
        json_extract_scalar(data, '$.data_nascimento_paciente') as data_nascimento_paciente,
        json_extract_scalar(data, '$.data_atendimento') as data_atendimento,
        json_extract_scalar(data, '$.sexo_paciente') as sexo_paciente,
        json_extract_scalar(data, '$.pcd') as pcd,
        json_extract_scalar(data, '$.nome_mae') as nome_mae,
        json_extract_scalar(data, '$.tipoAtendimento') as tipoAtendimento,
        json_extract_scalar(data, '$.especialidadeAtendimento') as especialidadeAtendimento,
        json_extract_scalar(data, '$.hr_nascimento_rn') as hr_nascimento_rn,
        datalake_loaded_at,
        source_updated_at
    from source
),

paciente_renomeado as (
    select
        -- Atendimento
        {{ process_null('numero_atendimento') }} as id_atendimento,
        {{ process_null('id_cnes') }} as id_cnes,
        safe.parse_datetime('%Y/%m/%d %H:%M:%S', data_atendimento) as atendimento_datahora,
        case 
            when tipoAtendimento like 'A' then 'AMBULATORIAL'
            when tipoAtendimento like 'B' then 'BUSCA ATIVA'
            when tipoAtendimento like 'E' then 'EXTERNO'
            when tipoAtendimento like 'H' then 'HOME CARE'
            when tipoAtendimento like 'I' then 'INTERNAÇÃO'
            when tipoAtendimento like 'S' then 'SUS - AIH'
            when tipoAtendimento like 'U' then 'URGÊNCIA'
            else tipoAtendimento
        end as atendimento_tipo,
        {{ process_null('especialidadeAtendimento') }} as atendimento_especialidade,

        -- Paciente
        {{ process_null('nome_paciente') }} as paciente_nome,
        {{ process_null('cns_paciente') }} as paciente_cns,
        {{ process_null('nome_social_paciente') }} as paciente_nome_social,
        safe.parse_date('%Y/%m/%d', data_nascimento_paciente) as paciente_data_nascimento,
        {{ process_null('sexo_paciente') }} as paciente_sexo,
        {{ process_null('pcd') }} as paciente_pcd,
        {{ process_null('nome_mae') }} as paciente_mae_nome,
        {{ process_null('hr_nascimento_rn') }} as recem_nascido_hora_nascimento,

        -- Metadados
        datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
        parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at) as updated_at,
        cast(datalake_loaded_at as date) as data_particao
    from paciente_json
),

paciente_deduplicado as (
    select *
    from paciente_renomeado
    qualify row_number() over (partition by id_atendimento, id_cnes order by updated_at desc) = 1
)

select 
    {{ dbt_utils.generate_surrogate_key(['id_atendimento', 'id_cnes']) }} as id_hci,
    *
from paciente_deduplicado