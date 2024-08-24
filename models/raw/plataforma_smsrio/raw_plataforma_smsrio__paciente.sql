{{
    config(
        alias="paciente",
        materialized="incremental",
        unique_key="id",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    events_from_window as (
        select *
        from {{ source("brutos_plataforma_smsrio_staging", "paciente_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by source_id order by source_updated_at desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1)
select
    -- Chave Primária
    safe_cast(NULLIF(source_id, '') as string) as id,
    
    -- Outras Chaves
    safe_cast(NULLIF(patient_cpf, '') as string) as cpf,
    safe_cast(NULLIF(NULLIF(data__cns_provisorio, ''), 'None') as string) as cns_lista,

    -- Informações Pessoais
    safe_cast(NULLIF(NULLIF(data__nome, ''), 'None') as string) as nome,
    safe_cast(NULLIF(NULLIF(data__nome_mae, ''), 'None') as string) as nome_mae,
    safe_cast(NULLIF(NULLIF(NULLIF(data__nome_pai, ''), 'None'), 'SEM INFORMACAO') as string) as nome_pai,
    safe_cast(NULLIF(NULLIF(data__sexo, ''), 'None') as string) as sexo,
    safe_cast(NULLIF(NULLIF(data__obito, ''), 'None') as string) as obito,
    safe_cast(NULLIF(NULLIF(data__dt_obito, ''), 'None') as date) as data_obito,
    safe_cast(NULLIF(NULLIF(data__racaCor, ''), 'None') as string) as raca_cor,

    -- Contato
    safe_cast(NULLIF(NULLIF(data__email, ''),'None') as string) as email,
    safe_cast(NULLIF(data__telefones, '') as string) as telefone_lista,

    -- Nascimento
    safe_cast(NULLIF(NULLIF(data__nacionalidade, ''), 'None') as string) as nacionalidade,
    safe_cast(NULLIF(NULLIF(data__dt_nasc, ''), 'None') as date) as data_nascimento,
    safe_cast(NULLIF(NULLIF(data__cod_mun_nasc, ''), 'None') as string) as codigo_municipio_nascimento,
    safe_cast(NULLIF(NULLIF(data__uf_nasc, ''), 'None') as string) as uf_nascimento,
    safe_cast(NULLIF(NULLIF(data__cod_pais_nasc, ''), 'None') as string) as codigo_pais_nascimento,

    -- Endereço
    safe_cast(NULLIF(NULLIF(data__end_tp_logrado_cod, ''), 'None') as string) as endereco_tipo_logradouro,
    safe_cast(NULLIF(NULLIF(data__end_cep, ''), 'None') as string) as endereco_cep,
    safe_cast(NULLIF(NULLIF(data__end_logrado, ''), 'None') as string) as endereco_logradouro,
    safe_cast(NULLIF(NULLIF(data__end_numero, ''), 'None') as string) as endereco_numero,
    safe_cast(NULLIF(NULLIF(data__end_comunidade, ''), 'None') as string) as endereco_comunidade,
    safe_cast(NULLIF(NULLIF(data__end_complem, ''), 'None') as string) as endereco_complemento,
    safe_cast(NULLIF(NULLIF(data__end_bairro, ''), 'None') as string) as endereco_bairro,
    safe_cast(NULLIF(NULLIF(data__cod_mun_res, ''), 'None') as string) as endereco_municipio_codigo,
    safe_cast(NULLIF(NULLIF(data__uf_res, ''), 'None') as string) as endereco_uf,

    -- Metadata columns
    safe_cast(NULLIF(ano_particao, '') as string) as ano_particao,
    safe_cast(NULLIF(mes_particao, '') as string) as mes_particao,
    safe_cast(NULLIF(data_particao, '') as date) as data_particao,
    safe_cast(NULLIF(source_updated_at, '') as timestamp) as updated_at
from latest_events

