{{
    config(
        alias="pesquisas_periodo",
        materialized="incremental",
        unique_key="id_pesquisa",
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
    from {{ source("brutos_prontuario_sarah_padi_staging", "pesquisas_periodo") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('pesquisa') }} as pesquisa,
        {{ base64_to_string('pesquisa_id') }} as pesquisa_id,
        {{ base64_to_string('entrevistador') }} as entrevistador,
        {{ base64_to_string('unidade_cnes') }} as unidade_cnes,
        {{ base64_to_string('unidade') }} as unidade,
        {{ base64_to_string('data') }} as data,
        {{ base64_to_string('hora') }} as hora,
        {{ base64_to_string('mes') }} as mes,
        {{ base64_to_string('escolaridade') }} as escolaridade,
        {{ base64_to_string('tipo_documento') }} as tipo_documento,
        {{ base64_to_string('raca_cor') }} as raca_cor,
        {{ base64_to_string('informou_idade') }} as informou_idade,
        {{ base64_to_string('idade') }} as idade,
        {{ base64_to_string('estado_civil') }} as estado_civil,
        {{ base64_to_string('renda_familiar') }} as renda_familiar,
        {{ base64_to_string('sexo') }} as sexo,
        {{ base64_to_string('genero') }} as genero,
        {{ base64_to_string('grau_parentesco') }} as grau_parentesco,
        {{ base64_to_string('exclusao') }} as exclusao,
        {{ base64_to_string('criterio_exclusao') }} as criterio_exclusao,
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
        safe_cast(pesquisa as string) as pesquisa,
        safe_cast(pesquisa_id as int64) as id_pesquisa,
        safe_cast(entrevistador as string) as entrevistador,
        safe_cast(unidade_cnes as string) as id_cnes,
        safe_cast(unidade as string) as unidade,
        safe_cast(data as date) as `data`,
        safe_cast(hora as time) as hora,
        safe_cast(mes as string) as mes,
        safe_cast(escolaridade as string) as escolaridade,
        safe_cast(tipo_documento as string) as tipo_documento,
        safe_cast(raca_cor as string) as raca_cor,
        safe_cast(informou_idade as string) as informou_idade,
        safe_cast(idade as int64) as idade,
        safe_cast(estado_civil as string) as estado_civil,
        safe_cast(renda_familiar as string) as renda_familiar,
        safe_cast(sexo as string) as sexo,
        safe_cast(genero as string) as genero,
        safe_cast(grau_parentesco as string) as grau_parentesco,
        safe_cast(exclusao as string) as exclusao,
        safe_cast(criterio_exclusao as string) as criterio_exclusao,
        safe_cast(pontuacao as int64) as pontuacao,
        safe_cast(quantidade as int64) as quantidade,

        -- Metadados
        safe_cast(extracted_at as datetime) as extracted_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao

    from base64_para_string
    qualify row_number() over (partition by pesquisa_id order by extracted_at desc) = 1 
)

select * from renomeado