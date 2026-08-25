{{
    config(
        alias="dtw__recem_nascido",
        materialized="incremental",
        unique_key="gid",
        partition_by={
            "field": "data_particao", 
            "data_type": "date", 
            "granularity": "day"
        },
        meta={"owner": "herian"}
    )
}}


{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    -- Seleciona eventos dos últimos 7 dias se for uma execução incremental
    events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "dtw__fat_recem_nascido_eventos") }}
        {% if is_incremental() %} where data_particao >= '{{seven_days_ago}}' {% endif %}
    ),
    -- Ranqueia os eventos por frescor dentro de cada grupo
    events_ranked_by_freshness as (
        select *, 
            row_number() over (partition by recem_gid order by datalake_loaded_at desc) as rank
        from events_from_window
    ),
    
    -- Seleciona apenas os eventos mais recentes de cada grupo
    latest_events as (
        select * 
        from events_ranked_by_freshness 
        where rank = 1
    )

select 
    -- Chaves Primárias e Estrangeiras
    {{ process_null('recem_gid') }} as gid,
    {{ process_null('estabelecimento_gid') }} as gid_estabelecimento,
    {{ process_null('boletim_gid') }} as gid_boletim,
    {{ process_null('boletim_gid_mae')}} as gid_boletim_mae,
    {{ process_null('fat_paciente_rede_id') }} as id_paciente_rede,
    {{ process_null('fat_paciente_rede_id_mae') }} as id_paciente_rede_mae,
    
    -- Campos 
    safe_cast(ren_datahoraparto as datetime) as parto_datahora,
    safe_cast(ren_dataobito as datetime) as obito_datahora,

    -- Parece um campo redudante, verificar se é necessário manter ambos
    trim({{ process_null('sexo') }}) as sexo,
    {{ process_null('ren_sexo') }} as recem_nascido_sexo,

    safe_cast(peso as float64) as peso,
    safe_cast(altura as float64) as altura,
    {{ process_null('ren_prematuro') }} as prematuro,

    -- Parece um campo redudante, verificar se é necessário manter ambos
    {{ process_null('possuimalformacaocongenita') }} as mal_formacao_congenita,
    {{ process_null('ren_malformacaocongenita') }} as ren_malformacao_congenita, 
    
    safe_cast(ren_perimetrotoracico as float64) as perimetro_toracico,
    safe_cast(ren_perimetrocefalico as float64) as perimetro_cefalico,
    {{ process_null('baixopeso') }} as baixo_peso,
    {{ process_null('microcefalia') }} as microcefalia,
    {{ process_null('doencacardiaca') }} as doenca_cardiaca,
    {{ process_null('doencainfecciosa') }} as doenca_infecciosa,
    {{ process_null('usoalcooldroga') }} as uso_alcool_droga,
    {{ process_null('gemelar') }} as gemelar,
    {{ process_null('ren_numerogemeo') }} as numero_gemeo,
    {{ process_null('semanasgestacao') }} as gestacao_semanas,
    {{ process_null('apgar_primeiro_minuto') }} as apgar_primeiro_minuto,
    {{ process_null('apgar_quinto_minuto') }} as apgar_quinto_minuto,
    {{ process_null('numero_dnv') }} as numero_dnv,
    {{ process_null('con_id') }} as con_id,
    {{ process_null('tip_id') }} as tip_id,
    {{ process_null('rcc_id') }} as rcc_id,

    -- Campos de Data e Horário
    safe_cast(data_registro as datetime) as registro_datahora,
    safe_cast(created_at as datetime) as created_at,
    safe_cast(updated_at as datetime) as updated_at,
    datetime(safe_cast(datalake_loaded_at as timestamp), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events