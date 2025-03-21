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
        from {{ source("brutos_plataforma_smsrio_staging", "_paciente_cadastro_eventos") }}
        {% if is_incremental() %} where datalake_loaded_at > '{{seven_days_ago}}' {% endif %}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by source_id order by source_updated_at desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1)
select
    -- Chave Primária
    safe_cast({{process_null('source_id')}} as string) as id,
    
    -- Outras Chaves
    safe_cast({{process_null('patient_cpf')}} as string) as cpf,
    safe_cast({{process_null('data__cns_provisorio')}} as string) as cns_lista,

    -- Informações Pessoais
    safe_cast({{process_null('data__nome')}} as string) as nome,
    safe_cast({{process_null('data__nome_mae')}} as string) as nome_mae,
    safe_cast({{process_null('data__nome_pai')}} as string) as nome_pai,
    safe_cast({{process_null('data__sexo')}} as string) as sexo,
    safe_cast({{process_null('data__obito')}} as string) as obito,
    safe_cast({{process_null('data__dt_obito')}} as date) as data_obito,
    safe_cast({{process_null('data__racaCor')}} as string) as raca_cor,

    -- Contato
    safe_cast({{process_null('data__email')}} as string) as email,
    safe_cast({{process_null('data__telefones')}} as string) as telefone_lista,

    -- Nascimento
    safe_cast({{process_null('data__nacionalidade')}} as string) as nacionalidade,
    safe_cast({{process_null('data__dt_nasc')}} as date) as data_nascimento,
    safe_cast({{process_null('data__cod_mun_nasc')}} as string) as codigo_municipio_nascimento,
    safe_cast({{process_null('data__uf_nasc')}} as string) as uf_nascimento,
    safe_cast({{process_null('data__cod_pais_nasc')}} as string) as codigo_pais_nascimento,

    -- Endereço
    safe_cast({{process_null('data__end_tp_logrado_nm')}} as string) as endereco_tipo_logradouro,
    safe_cast({{process_null('data__end_cep')}} as string) as endereco_cep,
    safe_cast({{process_null('data__end_logrado')}} as string) as endereco_logradouro,
    safe_cast({{process_null('data__end_numero')}} as string) as endereco_numero,
    safe_cast({{process_null('data__end_comunidade')}} as string) as endereco_comunidade,
    safe_cast({{process_null('data__end_complem')}} as string) as endereco_complemento,
    safe_cast({{process_null('data__end_bairro')}} as string) as endereco_bairro,
    safe_cast({{process_null('data__cod_mun_res')}} as string) as endereco_municipio_codigo,
    safe_cast({{process_null('data__uf_res')}} as string) as endereco_uf,

    -- Metadata columns
    safe_cast(datalake_loaded_at as date) as data_particao,
    timestamp_add(datetime(timestamp({{process_null('source_updated_at')}}), 'America/Sao_Paulo'),interval 3 hour)  as updated_at,
    datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at
from latest_events

