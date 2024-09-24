{{
    config(
        schema="intermediario_historico_clinico",
        alias="obito_vitai",
        materialized="table",
    )
}}
-- Cria tabela de episódios vitai com desfecho óbito para sinalização

-- Óbitos assinalados em episódios vitai
with alta_adm as (
    select * from 
    {{ref('raw_prontuario_vitai__alta')}}
),
alta_internacao as (
    select * from 
    {{ref('raw_prontuario_vitai__resumo_alta')}}
),
boletim as (
    select * from
    {{ref('raw_prontuario_vitai__boletim')}}
),
obitos as (
    select 
        case 
            when {{clean_numeric_string('boletim.cpf')}}  = '' then null 
            else {{clean_numeric_string('boletim.cpf')}} 
        end as cpf, 
        case 
            when {{clean_numeric_string('boletim.cns')}}  = '' then null 
            else {{clean_numeric_string('boletim.cns')}} 
        end as cns, 
        extract(
            date from datetime({{process_null('boletim.alta_data')}})
        ) as obito_data, 
        boletim.gid as gid_boletim_obito
    from boletim
    left join alta_adm
    on boletim.gid = alta_adm.gid_boletim
    left join alta_internacao
    on boletim.gid = alta_internacao.gid_boletim
    where (
    regexp_contains(alta_adm.abe_obs, '(CAD[Á|A]VER)|([O|Ó]BITO)') 
    or (
        {{process_null('alta_adm.obito_data')}} is not null 
    )
    )
    or alta_internacao.desfecho_internacao = 'ÓBITO'
),
-- FILTROS --
-- Filtro de obitos com episodios posteriores na vitai ou vitacare
ultimo_boletim_vitai as (
  select 
    cpf, 
    max(
        extract(
            date from datetime({{process_null('alta_data')}})
            )
    ) as ultima_entrada 
  from boletim
  group by 1
),
ultimo_boletim_vitacare as(
  select 
    cpf, 
    max(
        extract(
            date from datahora_fim
            )
    ) as ultima_entrada
  from  {{ref('raw_prontuario_vitacare__atendimento')}}
  group by 1
),
ultimo_boletim as(
    select cpf, max(ultima_entrada) as ultima_entrada
    from (
    select *
    from ultimo_boletim_vitacare
    union all 
    select * 
    from ultimo_boletim_vitai
    )
    group by 1
),
-- Final -- 
obitos_flags as (
    select obitos.*, 
        ultimo_boletim.ultima_entrada,
        IF(
            (ultimo_boletim.ultima_entrada > date_add(obito_data, interval 1 day)) OR (obito_data is null),
            1,
            0) as tem_boletim_pos_obito
    from obitos
    left join ultimo_boletim
    on obitos.cpf = ultimo_boletim.cpf
)
select 
    cpf, 
    max(obito_data) as obito_data,
    array_agg(distinct cns ignore nulls) as cns, 
    array_agg(distinct gid_boletim_obito ignore nulls) as gid_boletim_obito
from obitos_flags 
where tem_boletim_pos_obito = 0
group by 1