{{
    config(
        alias="atendimento_domiciliar_linhas_cuidado",
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
    from {{ source("brutos_prontuario_sarah_padi_staging", "atendimento_domiciliar_linhas_cuidado") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('atendimento_id') }} as atendimento_id,
        {{ base64_to_string('atendimento_registro') }} as atendimento_registro,
        {{ base64_to_string('atendimento_admissao') }} as atendimento_admissao,
        {{ base64_to_string('linha_cuidado') }} as linha_cuidado,
        {{ base64_to_string('cuidado_paliativo') }} as cuidado_paliativo,
        {{ base64_to_string('cids') }} as cids,
        {{ base64_to_string('unidade') }} as unidade,
        {{ base64_to_string('unidade_id') }} as unidade_id,
        {{ base64_to_string('servico') }} as servico,
        {{ base64_to_string('servico_id') }} as servico_id,
        {{ base64_to_string('rota') }} as rota,
        {{ base64_to_string('rota_id') }} as rota_id,
        {{ base64_to_string('bairro') }} as bairro,
        {{ base64_to_string('paciente_id') }} as paciente_id,
        {{ base64_to_string('paciente_cpf') }} as paciente_cpf,
        {{ base64_to_string('paciente_nome') }} as paciente_nome,
        {{ base64_to_string('avd') }} as avd,
        {{ base64_to_string('avd_no_periodo') }} as avd_no_periodo,
        {{ base64_to_string('avd_data') }} as avd_data,
        {{ base64_to_string('avd_elegivel') }} as avd_elegivel,
        {{ base64_to_string('avd_modalidade') }} as avd_modalidade,
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
        safe_cast(atendimento_registro as date) as atendimento_registro,
        safe_cast(atendimento_admissao as date) as atendimento_admissao,
        safe_cast(linha_cuidado as string) as linha_cuidado,
        safe_cast(cuidado_paliativo as string) as cuidado_paliativo,
        safe_cast(cids as string) as cids,
        safe_cast(unidade as string) as unidade,
        safe_cast(unidade_id as string) as unidade_id,
        safe_cast(servico as string) as servico,
        safe_cast(servico_id as int64) as servico_id,
        safe_cast(rota as string) as rota,
        safe_cast(rota_id as string) as rota_id,
        safe_cast(bairro as string) as bairro,
        safe_cast(paciente_id as string) as paciente_id,
        safe_cast(paciente_cpf as string) as paciente_cpf,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(avd as string) as avd,
        safe_cast(avd_no_periodo as string) as avd_no_periodo,
        safe_cast(avd_data as date) as avd_data,
        safe_cast(avd_elegivel as string) as avd_elegivel,
        safe_cast(avd_modalidade as string) as avd_modalidade,
        safe_cast(quantidade as int64) as quantidade,

        -- Metadados
        safe_cast(extracted_at as datetime) as extracted_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from base64_para_string
    qualify row_number() over(partition by atendimento_id order by extracted_at desc) = 1
)

select * from renomeado
