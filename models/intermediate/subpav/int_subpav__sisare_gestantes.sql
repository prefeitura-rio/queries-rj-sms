{{ config(
    schema = 'intermediario_plataforma_subpav',
    alias = 'sisare_gestantes',
    materialized = 'table'
) }}

with gestantes as (

    select
        cast(id_gestante as string) as id_gestante,
        cast(id_paciente as string) as id_paciente,
        cast(id_internacao as string) as id_internacao,
        safe_cast(ig as int64) as ig,
        safe_cast(id_tipo_gravidez as int64) as id_tipo_gravidez,
        safe_cast(id_via_parto as int64) as id_via_parto,
        dt_parto,
        safe_cast(id_desfecho_internacao as int64) as id_desfecho_internacao,
        safe_cast(id_desfecho_gestacao as int64) as id_desfecho_gestacao,
        safe_cast(puerpera as int64) as puerpera,
        status,
        created_at,
        updated_at,
        datalake_loaded_at
    from {{ ref('raw_plataforma_subpav_sisare__gestantes') }}

),

pacientes as (

    select
        cast(id_paciente as string) as id_paciente,
        regexp_replace(cast(cns as string), r'\D', '') as cns_digits,
        regexp_replace(cast(cpf as string), r'\D', '') as cpf_sisare,
        nome,
        telefone,
        dt_nascimento,
        nome_mae,
        id_raca_cor,
        sexo,
        cep,
        logradouro,
        num_logradouro,
        complemento,
        bairro,
        municipio,
        uf,
        observacoes,
        unidade_referencia,
        equipe_referencia,
        status as status_paciente,
        created_at as created_at_paciente,
        updated_at as updated_at_paciente,
        datalake_loaded_at as datalake_loaded_at_paciente
    from {{ ref('raw_plataforma_subpav_sisare__pacientes') }}
    qualify row_number() over (
        partition by id_paciente
        order by datalake_loaded_at desc, updated_at desc
    ) = 1

),

indice_cns_cpf as (

    select
        regexp_replace(cast(cns_particao as string), r'\D', '') as cns_digits,
        any_value(regexp_replace(trim(cast(cpf as string)), r'\D', '')) as cpf_indice
    from {{ ref('mart_historico_clinico_app__indice') }}
    where cns_particao is not null
      and {{ normalize_null("cpf") }} is not null
    group by 1

),

final as (

    select
        safe_cast(g.id_gestante as int64) as id_gestante,
        safe_cast(g.id_paciente as int64) as id_paciente,
        safe_cast(g.id_internacao as int64) as id_internacao,
        g.ig,
        g.id_tipo_gravidez,
        g.id_via_parto,
        g.dt_parto,
        g.id_desfecho_internacao,
        g.id_desfecho_gestacao,
        g.puerpera,
        p.cns_digits as cns,
        nullif(p.cpf_sisare, '') as cpf_sisare,
        nullif(i.cpf_indice, '') as cpf_indice,
        coalesce(nullif(p.cpf_sisare, ''), nullif(i.cpf_indice, '')) as cpf,
        case
            when nullif(p.cpf_sisare, '') is not null then 'sisare'
            when nullif(i.cpf_indice, '') is not null then 'indice'
            else null
        end as origem_cpf,
        p.nome,
        p.telefone,
        p.dt_nascimento,
        p.nome_mae,
        p.id_raca_cor,
        p.sexo,
        p.cep,
        p.logradouro,
        p.num_logradouro,
        p.complemento,
        p.bairro,
        p.municipio,
        p.uf,
        p.observacoes,
        p.unidade_referencia,
        p.equipe_referencia,
        g.status,
        g.created_at,
        g.updated_at,
        g.datalake_loaded_at,
        p.status_paciente,
        p.created_at_paciente,
        p.updated_at_paciente,
        p.datalake_loaded_at_paciente
    from gestantes g
    left join pacientes p
        on p.id_paciente = g.id_paciente
    left join indice_cns_cpf i
        on i.cns_digits = p.cns_digits

)

select *
from final