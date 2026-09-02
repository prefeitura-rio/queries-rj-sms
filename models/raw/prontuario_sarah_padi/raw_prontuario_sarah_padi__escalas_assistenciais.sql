{{
    config(
        alias="escalas_assistenciais",
        materialized="incremental",
        unique_key="id_atendimento",
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
    from {{ source("brutos_prontuario_sarah_padi_staging", "escalas_assistenciais") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('atendimento_id') }} as atendimento_id,
        {{ base64_to_string('paciente_id') }} as paciente_id,
        {{ base64_to_string('paciente_nome') }} as paciente_nome,
        {{ base64_to_string('paciente_sexo') }} as paciente_sexo,
        {{ base64_to_string('paciente_dtnasc') }} as paciente_dtnasc,
        {{ base64_to_string('paciente_idade') }} as paciente_idade,
        {{ base64_to_string('data') }} as data,
        {{ base64_to_string('escala_id') }} as escala_id,
        {{ base64_to_string('escala_nome') }} as escala_nome,
        {{ base64_to_string('escala_sigla') }} as escala_sigla,
        {{ base64_to_string('prestador_id') }} as prestador_id,
        {{ base64_to_string('prestador_nome') }} as prestador_nome,
        {{ base64_to_string('prestador_cargo') }} as prestador_cargo,
        {{ base64_to_string('avaliacao') }} as avaliacao,
        {{ base64_to_string('avaliacao_id') }} as avaliacao_id,
        {{ base64_to_string('pontuacao') }} as pontuacao,
        {{ base64_to_string('quantidade') }} as quantidade,
        extracted_at,
        ano_particao,
        mes_particao,
        data_particao
    from source
),

renomeado as (
    select
       safe_cast(atendimento_id as string) as id_atendimento,
       safe_cast(paciente_id as string) as id_paciente,
       safe_cast(paciente_nome as string) as paciente_nome,
       safe_cast(paciente_sexo as string) as paciente_sexo,
       safe_cast(paciente_dtnasc as date) as paciente_dtnasc,
       safe_cast(paciente_idade as int64) as paciente_idade,
       safe_cast(data as date) as `data`,
       safe_cast(escala_id as int64) as id_escala,
       safe_cast(escala_nome as string) as escala_nome,
       safe_cast(escala_sigla as string) as escala_sigla,
       safe_cast(prestador_id as string) as id_prestador,
       safe_cast(prestador_nome as string) as prestador_nome,
       safe_cast(prestador_cargo as string) as prestador_cargo,
       safe_cast(avaliacao as string) as avaliacao,
       safe_cast(avaliacao_id as int64) as id_avaliacao,
       safe_cast(pontuacao as int64) as pontuacao,
       safe_cast(quantidade as int64) as quantidade,

       -- Metadados
       safe_cast(extracted_at as datetime) as extracted_at,
       safe_cast(ano_particao as int64) as ano_particao,
       safe_cast(mes_particao as int64) as mes_particao,
       safe_cast(data_particao as date) as data_particao
    from base64_para_string
    qualify row_number() over (partition by atendimento_id order by extracted_at desc) = 1
)

select * from renomeado