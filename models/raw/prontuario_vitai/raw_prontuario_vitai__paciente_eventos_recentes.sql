{{ config(
    alias="paciente_eventos_recentes",
    materialized='incremental',
    partition_by={"field": "datalake__imported_at", "data_type": "timestamp"},
    partition_expiration_days=7,
    clustering_by=["gid"],
    tags=["vitai_db"]
) }}

{% set yesterday = (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).isoformat() %}

select
    safe_cast(gid as string) as gid,
    safe_cast(racacor as string) as raca_cor,
    safe_cast(nome_alternativo as string) as nome_alternativo,
    safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
    safe_cast(complemento as string) as complemento,
    safe_cast(cns as string) as cns,
    safe_cast(datanascimento as date format "YYYY-MM-DD") as data_nascimento,
    safe_cast(transex as string) as trans_genero,
    safe_cast(tipologradouro as string) as tipo_logradouro,
    safe_cast(nomelogradouro as string) as nome_logradouro,
    safe_cast(uf as string) as uf,
    safe_cast(nacionalidade as string) as nacionalidade,
    safe_cast(ocupacaocbo as string) as ocupacao_cbo,
    safe_cast(munipicio as string) as municipio,
    safe_cast(telefone as string) as telefone,
    safe_cast(nomemae as string) as nome_mae,
    safe_cast(sexo as string) as sexo,
    safe_cast(naturalidade as string) as naturalidade,
    safe_cast(datahora as timestamp) as data_hora,
    safe_cast(paisnascimento as string) as pais_nascimento,
    safe_cast(bairro as string) as bairro,
    safe_cast(dtobito as date format "YYYY-MM-DD") as data_obito,
    safe_cast(numero as string) as numero,
    safe_cast(idcidadao as string) as id_cidadao,
    safe_cast(cpf as string) as cpf,
    safe_cast(numero_prontuario as string) as numero_prontuario,
    safe_cast(nome as string) as nome,
    safe_cast(cliente as string) as cliente,
    safe_cast(datalake__imported_at as timestamp) as datalake__imported_at,
    safe_cast(data_particao as date format "YYYY-MM-DD") as data_particao
from {{ source("brutos_prontuario_vitai_staging", "paciente_eventos") }}
{% if is_incremental() %}
where data_particao > '{{yesterday}}'
{% endif %}