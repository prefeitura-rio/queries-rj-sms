{{
    config(
        alias="captacoes_ad_iniciadas",
        materialized="incremental",
        unique_key="id_captacao",
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
    from {{ source("brutos_prontuario_sarah_padi_staging", "captacoes_ad_iniciadas") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('captacao_id') }} as captacao_id,
        {{ base64_to_string('captacao_inicio') }} as captacao_inicio,
        {{ base64_to_string('captacao_fim') }} as captacao_fim,
        {{ base64_to_string('situacao') }} as situacao,
        {{ base64_to_string('situacao_id') }} as situacao_id,
        {{ base64_to_string('tipo') }} as tipo,
        {{ base64_to_string('tipo_id') }} as tipo_id,
        {{ base64_to_string('cpf') }} as cpf,
        {{ base64_to_string('sexo') }} as sexo,
        {{ base64_to_string('genero_id') }} as genero_id,
        {{ base64_to_string('genero') }} as genero,
        {{ base64_to_string('nome') }} as nome,
        {{ base64_to_string('nomesocial') }} as nomesocial,
        {{ base64_to_string('nascimento') }} as nascimento,
        {{ base64_to_string('idade') }} as idade,
        {{ base64_to_string('racacor') }} as racacor,
        {{ base64_to_string('racacor_id') }} as racacor_id,
        {{ base64_to_string('etnia') }} as etnia,
        {{ base64_to_string('etnia_id') }} as etnia_id,
        {{ base64_to_string('unidade_origem') }} as unidade_origem,
        {{ base64_to_string('unidade_origem_cnes') }} as unidade_origem_cnes,
        {{ base64_to_string('unidade_destino') }} as unidade_destino,
        {{ base64_to_string('unidade_destino_cnes') }} as unidade_destino_cnes,
        {{ base64_to_string('responsavel_inicio') }} as responsavel_inicio,
        {{ base64_to_string('responsavel_fim') }} as responsavel_fim,
        {{ base64_to_string('origem') }} as origem,
        {{ base64_to_string('origem_id') }} as origem_id,
        {{ base64_to_string('destino') }} as destino,
        {{ base64_to_string('destino_id') }} as destino_id,
        {{ base64_to_string('motivo') }} as motivo,
        {{ base64_to_string('motivo_id') }} as motivo_id,
        {{ base64_to_string('clinica') }} as clinica,
        {{ base64_to_string('clinica_id') }} as clinica_id,
        {{ base64_to_string('especialidade') }} as especialidade,
        {{ base64_to_string('especialidade_id') }} as especialidade_id,
        {{ base64_to_string('municipio_nat') }} as municipio_nat,
        {{ base64_to_string('municipio_nat_id') }} as municipio_nat_id,
        {{ base64_to_string('municipio_res') }} as municipio_res,
        {{ base64_to_string('municipio_res_id') }} as municipio_res_id,
        {{ base64_to_string('bairro') }} as bairro,
        {{ base64_to_string('quantidade') }} as quantidade,
        extracted_at,
        ano_particao,
        mes_particao,
        data_particao
    from source
),

renomeado as (
    select
       safe_cast(captacao_id as string) as id_captacao,
       safe_cast(captacao_inicio as date) as captacao_inicio,
       safe_cast(captacao_fim as date) as captacao_fim,
       safe_cast(situacao as string) as situacao,
       safe_cast(situacao_id as int64) as id_situacao,
       safe_cast(tipo as string) as tipo,
       safe_cast(tipo_id as int64) as id_tipo,
       safe_cast(cpf as string) as cpf,
       safe_cast(sexo as string) as sexo,
       safe_cast(genero_id as int64) as id_genero,
       safe_cast(genero as string) as genero,
       safe_cast(nome as string) as nome,
       safe_cast(nomesocial as string) as nomesocial,
       safe_cast(nascimento as date) as nascimento,
       safe_cast(idade as int64) as idade,
       safe_cast(racacor as string) as raca_cor,
       safe_cast(racacor_id as int64) as id_raca_cor,
       safe_cast(etnia as string) as etnia,
       safe_cast(etnia_id as int64) as id_etnia,
       safe_cast(unidade_origem as string) as unidade_origem,
       safe_cast(unidade_origem_cnes as string) as unidade_origem_cnes,
       safe_cast(unidade_destino as string) as unidade_destino,
       safe_cast(unidade_destino_cnes as string) as unidade_destino_cnes,
       safe_cast(responsavel_inicio as string) as responsavel_inicio,
       safe_cast(responsavel_fim as string) as responsavel_fim,
       safe_cast(origem as string) as origem,
       safe_cast(origem_id as int64) as id_origem,
       safe_cast(destino as string) as destino,
       safe_cast(destino_id as int64) as id_destino,
       safe_cast(motivo as string) as motivo,
       safe_cast(motivo_id as int64) as id_motivo,
       safe_cast(clinica as string) as clinica,
       safe_cast(clinica_id as int64) as id_clinica,
       safe_cast(especialidade as string) as especialidade,
       safe_cast(especialidade_id as int64) as id_especialidade,
       safe_cast(municipio_nat as string) as municipio_naturalidade,
       safe_cast(municipio_nat_id as int64) as id_municipio_naturalidade,
       safe_cast(municipio_res as string) as municipio_residencia,
       safe_cast(municipio_res_id as int64) as id_municipio_residencia,
       safe_cast(bairro as string) as bairro,
       safe_cast(quantidade as int64) as quantidade,
        
       -- Metadados
       safe_cast(extracted_at as string) as extracted_at,
       safe_cast(ano_particao as string) as ano_particao,
       safe_cast(mes_particao as string) as mes_particao,
       safe_cast(data_particao as date) as data_particao
    from base64_para_string
    qualify row_number() over (partition by captacao_id order by extracted_at desc) = 1
)

select * from renomeado