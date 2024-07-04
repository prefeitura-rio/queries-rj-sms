{{ config(
    alias="paciente",
    materialized='incremental',
    unique_key='gid',
    tags=["vitai_db"]
) }}

{% set seven_days_ago = (modules.datetime.date.today() - modules.datetime.timedelta(days=7)).isoformat() %}

with events_from_window as (
    select *
    from {{ source("brutos_prontuario_vitai_staging", "paciente_eventos") }}
    {% if is_incremental() %}
    where data_particao > '{{seven_days_ago}}'
    {% endif %}
),
events_ranked_by_freshness as (
    select *, row_number() over (partition by gid order by datahora desc) as rank
    from events_from_window
),
latest_events as (
    select *
    from events_ranked_by_freshness
    where rank = 1
)
select
    -- Chave Primária
    safe_cast(gid as string) as gid,

    -- Chaves Estrangeiras
    safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
    safe_cast(ocupacaocbo as string) as ocupacaocbo,

    -- Campos
    safe_cast(idcidadao as string) as idcidadao,
    safe_cast(cpf as string) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(racacor as string) as racacor,
    safe_cast(nome_alternativo as string) as nome_alternativo,
    safe_cast(complemento as string) as complemento,
    safe_cast(datanascimento as date format "YYYY-MM-DD") as datanascimento,
    safe_cast(transex as string) as transex,
    safe_cast(tipologradouro as string) as tipologradouro,
    safe_cast(nomelogradouro as string) as nomelogradouro,
    safe_cast(uf as string) as uf,
    safe_cast(nacionalidade as string) as nacionalidade,
    safe_cast(munipicio as string) as municipio,
    safe_cast(telefone as string) as telefone,
    safe_cast(nomemae as string) as nomemae,
    safe_cast(sexo as string) as sexo,
    safe_cast(naturalidade as string) as naturalidade,
    safe_cast(datahora as timestamp) as datahora,
    safe_cast(paisnascimento as string) as paisnascimento,
    safe_cast(bairro as string) as bairro,
    safe_cast(dtobito as date format "YYYY-MM-DD") as dtobito,
    safe_cast(numero as string) as numero,
    safe_cast(numero_prontuario as string) as numero_prontuario,
    safe_cast(nome as string) as nome,
    safe_cast(cliente as string) as cliente,
    safe_cast(datalake__imported_at as timestamp) as datalake__imported_at,
from latest_events