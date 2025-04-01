{{
    config(
        alias="paciente",
        materialized="incremental",
        unique_key="gid",
        tags=["every_30_min"],
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    old_events_from_window as (
        select *, cast(null as string) as created_at
        from {{ source("brutos_prontuario_vitai_staging", "paciente_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    new_events_from_window as (
        select * except(created_at), created_at
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__paciente_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    events_from_window as (
        select * from old_events_from_window
        union all
        select * from new_events_from_window
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by gid order by datahora desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1)
select
    -- Chave Primária
    safe_cast(gid as string) as gid,

    -- Chaves Estrangeiras
    safe_cast(estabelecimento_gid as string) as gid_estabelecimento,
    safe_cast(ocupacaocbo as string) as cbo_ocupacao,

    -- Campos
    safe_cast(idcidadao as string) as id_cidadao,
    if(gid="7EA7D533-31AE-4B5C-9939-B1C08015461A", "46402634816", safe_cast(cpf as string)) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(racacor as string) as raca_cor,
    safe_cast(nome_alternativo as string) as nome_alternativo,
    safe_cast(complemento as string) as complemento,
    safe_cast(datanascimento as date format "YYYY-MM-DD") as data_nascimento,
    safe_cast(transex as string) as transex,
    safe_cast(tipologradouro as string) as tipo_logradouro,
    safe_cast(nomelogradouro as string) as nome_logradouro,
    safe_cast(numero as string) as numero,
    safe_cast(uf as string) as uf,
    safe_cast(nacionalidade as string) as nacionalidade,
    safe_cast(munipicio as string) as municipio,
    safe_cast(telefone as string) as telefone,
    safe_cast(nomemae as string) as nome_mae,
    safe_cast(sexo as string) as sexo,
    safe_cast(naturalidade as string) as naturalidade,
    safe_cast(paisnascimento as string) as pais_nascimento,
    safe_cast(bairro as string) as bairro,
    safe_cast(dtobito as date format "YYYY-MM-DD") as data_obito,
    safe_cast(numero_prontuario as string) as numero_prontuario,
    safe_cast(nome as string) as nome,
    safe_cast(cliente as string) as cliente,
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
    
from latest_events
