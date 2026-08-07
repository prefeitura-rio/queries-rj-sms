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
        dt_parto as data_parto,
        id_desfecho_internacao,
        id_desfecho_gestacao,
        desfecho_gestacao
    from {{ ref('int_subpav__sisare_gestantes') }}
    where id_gestante is not null
      and id_paciente is not null
      and id_internacao is not null
      and id_desfecho_internacao in (1, 3)

),

pacientes_sisare as (

    select
        cast(id_paciente as string) as id_paciente,
        cpf,
        municipio as municipio_sisare,
        uf as uf_sisare
    from {{ ref('raw_plataforma_subpav_sisare__pacientes') }}
    where id_paciente is not null
    qualify row_number() over (
        partition by cast(id_paciente as string)
        order by datalake_loaded_at desc
    ) = 1

),

enderecos_historico as (

    select
        p.cpf,
        e.cidade as municipio_historico,
        e.estado as uf_historico,
        e.rank,
        e.datahora_ultima_atualizacao
    from {{ ref('mart_historico_clinico__paciente') }} p,
    unnest(p.endereco) as e
    where p.cpf is not null

),

enderecos_historico_deduplicado as (

    select *
    from enderecos_historico
    qualify row_number() over (
        partition by cpf
        order by
            case
                when upper(trim(municipio_historico)) = 'RIO DE JANEIRO'
                  and upper(trim(uf_historico)) = 'RJ'
                    then 1
                when upper(trim(uf_historico)) = 'RJ'
                  and (municipio_historico is null or trim(municipio_historico) = '')
                    then 2
                when municipio_historico is not null
                  or uf_historico is not null
                    then 3
                else 4
            end,
            datahora_ultima_atualizacao desc,
            rank
    ) = 1

),

pacientes_com_municipio as (

    select
        ps.id_paciente,
        coalesce(
            nullif(trim(h.municipio_historico), ''),
            nullif(trim(ps.municipio_sisare), '')
        ) as municipio,
        coalesce(
            nullif(trim(h.uf_historico), ''),
            nullif(trim(ps.uf_sisare), '')
        ) as uf
    from pacientes_sisare ps
    left join enderecos_historico_deduplicado h
        on h.cpf = ps.cpf

),

pacientes as (

    select
        id_paciente,
        municipio,
        uf
    from pacientes_com_municipio
    where
        -- Regra:
        -- A fonte principal para município e UF é o HCI.
        -- Quando município ou UF não estiverem preenchidos no HCI, utiliza-se o SISARE como fallback.
        -- Entram na tabela final:
        -- 1) pacientes com município final igual a Rio de Janeiro;
        -- 2) pacientes com município final vazio e UF final igual a RJ;
        -- 3) pacientes com município e UF finais vazios, para evitar perda por ausência completa de endereço nas duas fontes.
        -- Não entram pacientes de fora da cidade do Rio, com município preenchido diferente de Rio de Janeiro.
        upper(trim(municipio)) = 'RIO DE JANEIRO'
        or (
            upper(trim(uf)) = 'RJ'
            and (municipio is null or trim(municipio) = '')
        )
        or (
            (municipio is null or trim(municipio) = '')
            and (uf is null or trim(uf) = '')
        )

),

internacoes as (

    select
        cast(id_internacao as string) as id_internacao,
        dt_saida as data_alta_internacao,
        cast(unidade_atendimento as string) as cnes_maternidade_alta
    from {{ ref('raw_plataforma_subpav_sisare__internacoes') }}
    where id_internacao is not null
      and dt_saida >= date('2026-01-01')

),

altas as (

    select
        cast(id_internacao as string) as id_internacao,
        datetime(created_at) as data_hora_digitacao
    from {{ ref('raw_plataforma_subpav_sisare__vw_altas') }}
    where id_internacao is not null

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
        cpf,
        any_value({{ normalize_null("trim(cast(telefone as string))") }}) as telefone
    from {{ ref('int_prontuario_vitacare__paciente') }}
    where {{ normalize_null("trim(cast(telefone as string))") }} is not null
    group by 1

),

vitai_tel as (

    select
        {{ clean_numeric("cast(cpf as string)") }} as cpf,
        any_value({{ normalize_null("trim(cast(telefone as string))") }}) as telefone
    from {{ ref('raw_prontuario_vitai__paciente') }}
    where {{ clean_numeric("cast(cpf as string)") }} is not null
      and {{ normalize_null("trim(cast(telefone as string))") }} is not null
    group by 1

),

cegonha_tel as (

    select
        cpf,
        array_agg(
            telefone.telefone_original
            order by cast(telefone.prioridade as int64)
            limit 1
        )[offset(0)] as telefone_cegonha
    from {{ ref('mart_iplanrio__siscegonha_agendamento_maternidade') }},
    unnest(telefones_gestante) as telefone
    where {{ normalize_null("trim(cast(cpf as string))") }} is not null
      and telefone.origem = 'cegonha'
      and {{ normalize_null("trim(cast(telefone.telefone_original as string))") }} is not null
    group by 1

),

base as (

    select
        g.cpf,
        g.nome,
        p.municipio,
        p.uf,
        a.data_hora_digitacao,
        i.data_alta_internacao,
        regexp_replace(i.cnes_maternidade_alta, r'\D', '') as cnes_maternidade_alta,
        e.nome_maternidade_alta,
        g.data_parto,
        g.id_desfecho_gestacao,
        g.desfecho_gestacao,
        cg.telefone_cegonha,
        vt.telefone as telefone_vitacare,
        vi.telefone as telefone_vitai
    from gestantes g
    inner join pacientes p
        on p.id_paciente = g.id_paciente
    inner join internacoes i
        on i.id_internacao = g.id_internacao
    left join altas a
        on a.id_internacao = g.id_internacao
    left join estabelecimento e
        on e.cnes_maternidade_alta = regexp_replace(i.cnes_maternidade_alta, r'\D', '')
    left join cegonha_tel cg
        on cg.cpf = g.cpf
    left join vitacare_tel vt
        on vt.cpf = g.cpf
    left join vitai_tel vi
        on vi.cpf = g.cpf

),

telefones_explodidos as (

    select
        b.cpf,
        b.nome,
        b.municipio,
        b.uf,
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
        struct(b.telefone_cegonha  as telefone, 'cegonha'   as origem, 1 as prioridade),
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
        municipio,
        uf,
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
        municipio,
        uf,
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
        municipio,
        uf,
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