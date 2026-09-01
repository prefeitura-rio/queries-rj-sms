{{
    config(
        alias="procedimentos_realizados",
        materialized="incremental",
        unique_key=['id_atendimento', 'id_procedimento', 'procedimento_data'],
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
    from {{ source("brutos_prontuario_sarah_padi_staging", "procedimentos_realizados") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('numero_atendimento') }} as numero_atendimento,
        {{ base64_to_string('data_procedimento') }} as data_procedimento,
        {{ base64_to_string('hora_procedimento') }} as hora_procedimento,
        {{ base64_to_string('paciente_nome') }} as paciente_nome,
        {{ base64_to_string('paciente_cpf') }} as paciente_cpf,
        {{ base64_to_string('cargo') }} as cargo,
        {{ base64_to_string('profissional_id') }} as profissional_id,
        {{ base64_to_string('nome_profissional') }} as nome_profissional,
        {{ base64_to_string('cpf_profissional') }} as cpf_profissional,
        {{ base64_to_string('cns_profissional') }} as cns_profissional,
        {{ base64_to_string('procedimento') }} as procedimento,
        {{ base64_to_string('procedimento_id') }} as procedimento_id,
        {{ base64_to_string('sexo') }} as sexo,
        {{ base64_to_string('municipio_ibge') }} as municipio_ibge,
        {{ base64_to_string('municipio_uf') }} as municipio_uf,
        {{ base64_to_string('uf') }} as uf,
        {{ base64_to_string('clinica') }} as clinica,
        {{ base64_to_string('clinica_especialidade') }} as clinica_especialidade,
        {{ base64_to_string('local') }} as local,
        {{ base64_to_string('unidade') }} as unidade,
        {{ base64_to_string('quantidade') }} as quantidade,
        extracted_at,
        ano_particao,
        mes_particao,
        data_particao
    from source
),

renomeado as (
    select
        safe_cast(numero_atendimento as string) as id_atendimento,
        safe_cast(procedimento_id as int64) as id_procedimento,
        safe_cast(procedimento as string) as procedimento,
        safe_cast(data_procedimento as date) as procedimento_data,
        safe_cast(hora_procedimento as time) as procedimento_hora,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_cpf as string) as paciente_cpf,
        safe_cast(cargo as string) as cargo,
        safe_cast(profissional_id as int64) as id_profissional,
        safe_cast(nome_profissional as string) as nome_profissional,
        safe_cast(cpf_profissional as string) as cpf_profissional,
        safe_cast(cns_profissional as string) as cns_profissional,
        safe_cast(sexo as string) as sexo,
        safe_cast(municipio_ibge as int64) as municipio_ibge,
        safe_cast(municipio_uf as string) as municipio_uf,
        safe_cast(uf as string) as uf,
        safe_cast(clinica as string) as clinica,
        safe_cast(clinica_especialidade as string) as clinica_especialidade,
        safe_cast(`local` as string) as `local`,
        safe_cast(unidade as string) as unidade,
        safe_cast(quantidade as int64) as quantidade,

        -- Metadados
        safe_cast(extracted_at as datetime) as extracted_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
        
    from base64_para_string
    qualify row_number() over (
        partition by 
            numero_atendimento, 
            procedimento_id, 
            data_procedimento
        order by extracted_at desc) = 1
)

select * from renomeado