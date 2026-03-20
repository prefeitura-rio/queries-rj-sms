{{
    config(
        schema="brutos_siclom_api",
        alias="pep",
        tags=["siclom"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        unique_key="id"
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with 

source as (
    select * 
    from {{ source('brutos_siclom_api_staging', 'pep') }}
    {% if is_incremental() %}
    where date(data_particao) >= date('{{ last_partition }}')
    {% endif %}
),

pep as (
    select
        -- Identificação do paciente
        {{ process_null('CPF') }} as paciente_cpf,
        {{ process_null('NOME_CIVIL') }} as paciente_nome,
        {{ process_null('NOME_SOCIAL') }} as paciente_nome_social,

        -- Dados demográficos
        {{ process_null('SITUACAO_ESTRANGEIRO') }} as situacao_estrangeiro,
        {{ process_null('st_habitantes_fronteira') }} as habitante_fronteira,
        {{ process_null('IDENTIDADE_DE_GENERO') }} as identidade_genero,
        {{ process_null('SITUACAO_DE_RUA') }} as situacao_rua,
        {{ process_null('PRIVADA_DE_LIBERDADE') }} as privada_liberdade,
    
        -- Dados de exposição 
        safe_cast(DATA_DA_EXPOSICAO as datetime) as exposicao_data,
        {{ process_null('CIRCUNSTANCIA_DA_EXPOSICAO') }} as circunstancia_exposicao,
        {{ process_null('ORIGEM_DO_ACOMPANHAMENTO') }} as origem_acompanhamento,
        {{ process_null('PESSOA_FONTE') }} as pessoa_fonte,
        
        -- Local de Atendimento
        {{ process_null('SERVICO_DE_ATENDIMENTO') }} as servico_atendimento,
        {{ process_null('UF_DISPENSADOR') }} as dispensador_uf,
        {{ process_null('DISPENSADOR') }} as dispensador,
        
        -- Prescrição
        {{ process_null('st_contraindicacao_esquema') }} as contraindicacao_esquema,
        safe_cast(DATA_DA_DISPENSA as datetime) as dispensa_data,
        {{ process_null('ESQUEMA') }} as esquema,
        safe_cast(DURACAO as float64) as duracao,
        {{ process_null('TP_PROFISSIONAL') }} as profissional_tipo,
        safe_cast(DATA_DA_PRESCRICAO as datetime) as prescricao_data,

        -- Testagem
        {{ process_null('tp_testagem_hiv') }} as testagem_hiv,
        {{ process_null('st_esquema_alternativo') }} as esquema_alternativo,

        cast(extracted_at as datetime) as extraido_em,
        date(data_particao) as data_particao
    from source 
)


select 
    {{
        dbt_utils.generate_surrogate_key(
            [
            "paciente_cpf",
            "paciente_nome",
            "exposicao_data",
            "esquema",
            "prescricao_data"
            ]            
        )
    }} as id,
    *
from pep
qualify row_number() over (partition by id order by extraido_em desc)=1

