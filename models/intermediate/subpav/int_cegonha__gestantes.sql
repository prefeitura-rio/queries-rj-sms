{{ config(
    schema = 'intermediario_cegonha',
    alias = 'gestantes',
    materialized = 'table'
) }}

with gestantes as (

    select
        cast(id_gestante as string) as id_gestante,
        regexp_replace(cast(num_cns as string), r'\D', '') as num_cns_digits,
        regexp_replace(cast(cpf as string), r'\D', '') as cpf_cegonha,
        nme_nome,
        flg_ativo,
        created_at,
        updated_at,
        id_user,
        id_raca_cor,
        num_cns_video,
        datalake_loaded_at
    from {{ ref('raw_plataforma_subpav_cegonha__gestantes') }}

),

-- Enriquecendo o preenchimendo de CPFs dos pacientes atraves do CNS
indice_cns_cpf as (

    select
        regexp_replace(cast(cns_particao as string), r'\D', '') as num_cns_digits,
        any_value(regexp_replace(trim(cast(cpf as string)), r'\D', '')) as cpf_indice
    from {{ ref('mart_historico_clinico_app__indice') }}
    where cns_particao is not null
      and cpf is not null
      and trim(cast(cpf as string)) <> ''
      and lower(trim(cast(cpf as string))) not in ('none', 'nan')
    group by 1

),

final as (

    select
        safe_cast(g.id_gestante as int64) as id_gestante,
        g.num_cns_digits as num_cns,
        nullif(g.cpf_cegonha, '') as cpf_cegonha,
        nullif(i.cpf_indice, '') as cpf_indice,
        coalesce(nullif(g.cpf_cegonha, ''), nullif(i.cpf_indice, '')) as cpf,
        case
            when nullif(g.cpf_cegonha, '') is not null then 'cegonha'
            when nullif(i.cpf_indice, '') is not null then 'indice'
            else null
        end as origem_cpf,
        g.nme_nome,
        g.flg_ativo,
        g.created_at,
        g.updated_at,
        g.id_user,
        g.id_raca_cor,
        g.num_cns_video,
        g.datalake_loaded_at
    from gestantes g
    left join indice_cns_cpf i
        on i.num_cns_digits = g.num_cns_digits

)

select *
from final