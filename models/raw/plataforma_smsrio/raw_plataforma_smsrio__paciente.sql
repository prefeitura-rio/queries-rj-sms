{{
    config(
        alias="paciente",
        materialized="incremental",
        unique_key="paciente_cpf",
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
        select *, row_number() over (partition by patient_cpf order by source_updated_at desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1)
select
    safe_cast(patient_cpf as string) as paciente_cpf,
    safe_cast(data__nome as string) as nome,
    safe_cast(data__nome_mae as string) as nome_mae,
    safe_cast(data__nome_pai as string) as nome_pai,
    safe_cast(data__dt_nasc as string) as dt_nasc,
    safe_cast(data__sexo as string) as sexo,
    safe_cast(data__racaCor as string) as raca_cor,
    safe_cast(data__nacionalidade as string) as nacionalidade,
    safe_cast(data__obito as string) as obito,
    safe_cast(data__dt_obito as string) as dt_obito,
    safe_cast(data__end_tp_logrado_cod as string) as end_tp_logrado_cod,
    safe_cast(data__end_logrado as string) as end_logrado,
    safe_cast(data__end_numero as string) as end_numero,
    safe_cast(data__end_comunidade as string) as end_comunidade,
    safe_cast(data__end_complem as string) as end_complem,
    safe_cast(data__end_bairro as string) as end_bairro,
    safe_cast(data__end_cep as string) as end_cep,
    safe_cast(data__cod_mun_res as string) as cod_mun_res,
    safe_cast(data__uf_res as string) as uf_res,
    safe_cast(data__cod_mun_nasc as string) as cod_mun_nasc,
    safe_cast(data__uf_nasc as string) as uf_nasc,
    safe_cast(data__cod_pais_nasc as string) as cod_pais_nasc,
    safe_cast(data__email as string) as email,
    safe_cast(data__timestamp as string) as timestamp,
    safe_cast(data__cns_provisorio as string) as cns_provisorio,
    safe_cast(data__telefones as string) as telefones,
    safe_cast(source_updated_at as string) as updated_at,
    safe_cast(ano_particao as string) as ano_particao,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(data_particao as date) as data_particao
from latest_events

