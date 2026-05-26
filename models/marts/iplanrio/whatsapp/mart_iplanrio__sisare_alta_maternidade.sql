{{ config(
    schema = 'projeto_whatsapp',
    alias = 'sisare_alta_maternidade',
    materialized = 'table',
    cluster_by = ['cpf', 'cnes_maternidade_alta']
) }}

with gestantes as (

    select
        cast(id_gestante as string) as id_gestante,
        cast(id_paciente as string) as id_paciente,
        cast(id_internacao as string) as id_internacao,
        cpf,
        nome,
        telefone as telefone_sisare,
        dt_parto as data_parto,
        id_desfecho_internacao,
        id_desfecho_gestacao
    from {{ ref('int_subpav__sisare_gestantes') }}
    where id_gestante is not null
      and id_paciente is not null
      and id_internacao is not null
      and id_desfecho_internacao in (1, 3)

),

internacoes as (

    select
        cast(id_internacao as string) as id_internacao,
        dt_saida as data_alta_internacao,
        cast(unidade_atendimento as string) as cnes_maternidade_alta
    from {{ ref('raw_plataforma_subpav_sisare__internacoes') }}
    where id_internacao is not null
      and dt_saida >= date('2026-01-01')  -- filtra pelos dados a partir de 2026

),

altas as (

    select
        cast(id_internacao as string) as id_internacao,
        datetime(created_at) as data_hora_digitacao
    from {{ ref('raw_plataforma_subpav_sisare__vw_altas') }}
    where id_internacao is not null

),

desfechos_gestacao as (

    select
        id_desfecho_gestacao,
        descricao as desfecho_gestacao
    from {{ ref('raw_plataforma_subpav_sisare__desfechos_gestacao') }}

),

estabelecimento as (

    select
        cast(id_cnes as string) as cnes_maternidade_alta,
        any_value(nome_limpo) as nome_maternidade_alta
    from {{ ref('int_gdb_cnes__estabelecimento') }}
    where id_cnes is not null
    group by 1

),

vitacare_tel as (

    select
        regexp_replace(cast(cpf as string), r'\D', '') as cpf,
        any_value({{ normalize_null("trim(cast(telefone as string))") }}) as telefone
    from {{ ref('raw_prontuario_vitacare__paciente') }}
    where {{ normalize_null("trim(cast(cpf as string))") }} is not null
      and {{ normalize_null("trim(cast(telefone as string))") }} is not null
    group by 1

),

vitai_tel as (

    select
        regexp_replace(cast(cpf as string), r'\D', '') as cpf,
        any_value({{ normalize_null("trim(cast(telefone as string))") }}) as telefone
    from {{ ref('raw_prontuario_vitai__paciente') }}
    where {{ normalize_null("trim(cast(cpf as string))") }} is not null
      and {{ normalize_null("trim(cast(telefone as string))") }} is not null
    group by 1

),

base as (

    select
        g.cpf,
        g.nome,
        a.data_hora_digitacao,
        i.data_alta_internacao,
        regexp_replace(i.cnes_maternidade_alta, r'\D', '') as cnes_maternidade_alta,
        e.nome_maternidade_alta,
        g.data_parto,
        g.id_desfecho_gestacao,
        d.desfecho_gestacao,
        g.telefone_sisare,
        vt.telefone as telefone_vitacare,
        vi.telefone as telefone_vitai
    from gestantes g
    inner join internacoes i
        on i.id_internacao = g.id_internacao
    left join altas a
        on a.id_internacao = g.id_internacao
    left join desfechos_gestacao d
        on d.id_desfecho_gestacao = g.id_desfecho_gestacao
    left join estabelecimento e
        on e.cnes_maternidade_alta = regexp_replace(i.cnes_maternidade_alta, r'\D', '')
    left join vitacare_tel vt
        on vt.cpf = g.cpf
    left join vitai_tel vi
        on vi.cpf = g.cpf

),

telefones_explodidos as (

    select
        b.cpf,
        b.nome,
        b.data_hora_digitacao,
        b.data_alta_internacao,
        b.cnes_maternidade_alta,
        b.nome_maternidade_alta,
        b.data_parto,
        b.id_desfecho_gestacao,
        b.desfecho_gestacao,
        tel.telefone,
        tel.origem,
        tel.prioridade
    from base b,
    unnest([
        struct(b.telefone_sisare   as telefone, 'sisare'    as origem, 1 as prioridade),
        struct(b.telefone_vitacare as telefone, 'vitacare'  as origem, 2 as prioridade),
        struct(b.telefone_vitai    as telefone, 'vitai'     as origem, 3 as prioridade)
    ]) as tel
    where {{ normalize_null("trim(tel.telefone)") }} is not null

),

telefones_deduplicados as (

    select *
    from telefones_explodidos
    qualify row_number() over (
        partition by cpf, regexp_replace(telefone, r'\D', '')
        order by prioridade
    ) = 1

),

final as (

    select
        cpf,
        nome,
        data_hora_digitacao,
        data_alta_internacao,
        cnes_maternidade_alta,
        nome_maternidade_alta,
        data_parto,
        id_desfecho_gestacao,
        desfecho_gestacao,
        array_agg(
            struct(
                telefone as telefone_original,
                origem,
                cast(prioridade as string) as prioridade,
                {{ padroniza_telefone_whatsapp('telefone') }}.telefone_valido_whatsapp as telefone_valido_whatsapp,
                {{ padroniza_telefone_whatsapp('telefone') }}.motivo_invalidacao_telefone as motivo_invalidacao_telefone
            )
            order by prioridade
        ) as telefones_gestante
    from telefones_deduplicados
    group by
        cpf,
        nome,
        data_hora_digitacao,
        data_alta_internacao,
        cnes_maternidade_alta,
        nome_maternidade_alta,
        data_parto,
        id_desfecho_gestacao,
        desfecho_gestacao

),

excecao_disparo_puerperas as (

    select
        cpf,
        nome,
        cast(null as datetime) as data_hora_digitacao,
        data_alta_internacao,
        cnes_maternidade_alta,
        nome_maternidade_alta,
        data_parto,
        id_desfecho_gestacao,
        desfecho_gestacao,
        telefones_gestante
    from {{ source("projeto_whatsapp", "excecao_disparo_puerperas") }}

)

select *
from final

union all

select *
from excecao_disparo_puerperas