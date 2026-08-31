{{ config(
    schema = 'projeto_whatsapp',
    alias = 'alta_maternidade',
    materialized = 'table',
    partition_by = {
        'field': 'data_alta_internacao',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by = ['cpf', 'cnes_maternidade_alta']
) }}

-- Telefones por fonte e prioridade:
-- SISARE: SisCegonha (1) > Vitacare (2) > Vitai (3), pois consideramos esses números mais atualizados que o registrado na internação.
-- A mesma ordem e aplicada aos eventos de todos os prontuarios.

with eventos_obstetricos_evidencias as (

    select
        id_evento_obstetrico,
        cast(id_paciente as string) as id_paciente,
        cast(id_hci as string) as id_hci,
        cpf,
        fonte,
        subtipo_evento,
        origem_data_alta,
        data_parto,
        data_alta_internacao,
        cnes_estabelecimento,
        id_desfecho_gestacao,
        desfecho_gestacao,
        loaded_at
    from {{ ref('int_historico_clinico__gestacoes__eventos_obstetricos') }}
    where tipo_evento = 'parto'
      and fonte in ('sisare', 'prontuaRio', 'mv', 'vitai')

),

eventos_obstetricos as (

    -- O modelo canonico preserva cada evidencia obstetrica. Para o disparo, consolidamos
    -- essas evidencias em um unico parto materno por prontuario/atendimento.
    select *
    from eventos_obstetricos_evidencias
    qualify row_number() over (
        partition by
            fonte,
            coalesce(id_hci, id_evento_obstetrico)
        order by
            case subtipo_evento
                when 'nascimento_rn' then 1
                when 'registro_parto' then 2
                when 'desfecho_internacao' then 3
                when 'procedimento_parto' then 4
                when 'cid_parto' then 5
                else 99
            end,
            case when data_parto is null then 1 else 0 end,
            loaded_at desc,
            id_evento_obstetrico
    ) = 1

),

pacientes_hci as (

    select
        cpf,
        coalesce(
            nullif(trim(dados.nome_social), ''),
            nullif(trim(dados.nome), '')
        ) as nome_hci
    from {{ ref('mart_historico_clinico__paciente') }}
    where cpf is not null

),

pacientes_sisare as (

    select
        cast(id_paciente as string) as id_paciente,
        cpf,
        nome as nome_sisare,
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
        ps.nome_sisare,
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
        nome_sisare,
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

altas as (

    select
        cast(id_internacao as string) as id_internacao,
        datetime(created_at) as data_hora_digitacao
    from {{ ref('raw_plataforma_subpav_sisare__vw_altas') }}
    where id_internacao is not null
    qualify row_number() over (
        partition by cast(id_internacao as string)
        order by created_at desc
    ) = 1

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
        g.id_evento_obstetrico,
        g.subtipo_evento,
        g.origem_data_alta,
        g.cpf,
        coalesce(ph.nome_hci, p.nome_sisare) as nome,
        p.municipio,
        p.uf,
        a.data_hora_digitacao,
        g.data_alta_internacao,
        g.cnes_estabelecimento as cnes_maternidade_alta,
        e.nome_maternidade_alta,
        g.data_parto,
        g.id_desfecho_gestacao,
        g.desfecho_gestacao,
        cg.telefone_cegonha,
        vt.telefone as telefone_vitacare,
        vi.telefone as telefone_vitai,
        'sisare' as prontuario_origem,
        cast(g.loaded_at as datetime) as datalake_loaded_at
    from eventos_obstetricos g
    inner join pacientes p
        on p.id_paciente = g.id_paciente
    left join pacientes_hci ph
        on ph.cpf = g.cpf
    left join altas a
        on a.id_internacao = g.id_hci
    left join estabelecimento e
        on e.cnes_maternidade_alta = g.cnes_estabelecimento
    left join cegonha_tel cg
        on cg.cpf = g.cpf
    left join vitacare_tel vt
        on vt.cpf = g.cpf
    left join vitai_tel vi
        on vi.cpf = g.cpf
    where g.fonte = 'sisare'
      and g.data_alta_internacao >= date('2026-01-01')

),

prontuario_cadastro_dedup as (
    select *
    from {{ ref('raw_prontuario_prontuaRio__internacao_cadastro') }}
    qualify row_number() over (partition by gid_prontuario order by loaded_at desc) = 1
),

prontuario_altas as (
    select
        ep.id_evento_obstetrico,
        ep.subtipo_evento,
        ep.origem_data_alta,
        ep.id_hci as gid_prontuario,
        ep.data_parto,
        ep.data_alta_internacao,
        ep.cnes_estabelecimento as cnes_prontuario,
        ep.cpf,
        coalesce(ph.nome_hci, nullif(trim(cad.paciente_nome), '')) as nome,
        coalesce(nullif(trim(cad.endereco_municipio), ''), 'Rio de Janeiro') as municipio,
        coalesce(nullif(trim(cad.endereco_uf), ''), 'RJ') as uf,
        ep.loaded_at as datalake_loaded_at
    from eventos_obstetricos ep
    left join prontuario_cadastro_dedup cad on cad.gid_prontuario = ep.id_hci
    left join pacientes_hci ph on ph.cpf = ep.cpf
    where ep.fonte = 'prontuaRio'
      and ep.cpf is not null
),

base_prontuario as (
    select
        pa.id_evento_obstetrico,
        pa.subtipo_evento,
        pa.origem_data_alta,
        pa.cpf,
        pa.nome,
        pa.municipio,
        pa.uf,
        cast(null as datetime) as data_hora_digitacao,
        pa.data_alta_internacao,
        regexp_replace(pa.cnes_prontuario, r'\D', '') as cnes_maternidade_alta,
        e.nome_maternidade_alta,
        pa.data_parto,
        cast(null as int64) as id_desfecho_gestacao,
        cast(null as string) as desfecho_gestacao,
        cg.telefone_cegonha,
        vt.telefone as telefone_vitacare,
        vi.telefone as telefone_vitai,
        'prontuaRio' as prontuario_origem,
        pa.datalake_loaded_at
    from prontuario_altas pa
    left join estabelecimento e
        on e.cnes_maternidade_alta = regexp_replace(pa.cnes_prontuario, r'\D', '')
    left join cegonha_tel cg on cg.cpf = pa.cpf
    left join vitacare_tel vt on vt.cpf = pa.cpf
    left join vitai_tel vi on vi.cpf = pa.cpf
),

mv_admissao_dedup as (
    select
        * except(id_hci),
        {{ dbt_utils.generate_surrogate_key([
            "id_atendimento",
            "id_cnes"
        ]) }} as id_hci
    from {{ ref('raw_prontuario_mv__admissao') }}
    qualify row_number() over (
        partition by cast(id_atendimento as string), cast(id_cnes as string)
        order by updated_at desc
    ) = 1
),

mv_alta_dedup as (
    select *
    from {{ ref('raw_prontuario_mv__alta') }}
    qualify row_number() over (partition by id_hci order by updated_at desc) = 1
),

mv_atendimento_dedup as (
    select *
    from {{ ref('raw_prontuario_mv__atendimento') }}
    qualify row_number() over (partition by id_hci order by updated_at desc) = 1
),

mv_gestante_dedup as (
    select *
    from {{ ref('raw_prontuario_mv__gestante') }}
    qualify row_number() over (partition by id_hci order by updated_at desc) = 1
),

mv_altas as (
    select
        ep.id_evento_obstetrico,
        ep.subtipo_evento,
        ep.origem_data_alta,
        ep.cpf,
        coalesce(
            ph.nome_hci,
            nullif(trim(adm.paciente_nome_social), ''),
            nullif(trim(adm.paciente_nome), ''),
            nullif(trim(alta.paciente_nome_social), ''),
            nullif(trim(alta.paciente_nome), ''),
            nullif(trim(ate.paciente_nome_social), ''),
            nullif(trim(ate.paciente_nome), ''),
            nullif(trim(ges.paciente_nome_social), ''),
            nullif(trim(ges.paciente_nome), '')
        ) as nome,
        'Rio de Janeiro' as municipio,
        'RJ' as uf,
        ep.data_parto,
        ep.data_alta_internacao,
        ep.cnes_estabelecimento as cnes_mv,
        ep.loaded_at as datalake_loaded_at
    from eventos_obstetricos ep
    left join mv_admissao_dedup adm on adm.id_hci = ep.id_hci
    left join mv_alta_dedup alta on alta.id_hci = ep.id_hci
    left join mv_atendimento_dedup ate on ate.id_hci = ep.id_hci
    left join mv_gestante_dedup ges on ges.id_hci = ep.id_hci
    left join pacientes_hci ph on ph.cpf = ep.cpf
    where ep.fonte = 'mv'
      and ep.cpf is not null
),

base_mv as (
    select
        ma.id_evento_obstetrico,
        ma.subtipo_evento,
        ma.origem_data_alta,
        ma.cpf,
        ma.nome,
        ma.municipio,
        ma.uf,
        cast(null as datetime) as data_hora_digitacao,
        ma.data_alta_internacao,
        regexp_replace(ma.cnes_mv, r'\D', '') as cnes_maternidade_alta,
        e.nome_maternidade_alta,
        ma.data_parto,
        cast(null as int64) as id_desfecho_gestacao,
        cast(null as string) as desfecho_gestacao,
        cg.telefone_cegonha,
        vt.telefone as telefone_vitacare,
        vi.telefone as telefone_vitai,
        'mv' as prontuario_origem,
        ma.datalake_loaded_at
    from mv_altas ma
    left join estabelecimento e
        on e.cnes_maternidade_alta = regexp_replace(ma.cnes_mv, r'\D', '')
    left join cegonha_tel cg on cg.cpf = ma.cpf
    left join vitacare_tel vt on vt.cpf = ma.cpf
    left join vitai_tel vi on vi.cpf = ma.cpf
),

vitai_boletim_dedup as (
    select *
    from {{ ref('raw_prontuario_vitai__boletim') }}
    qualify row_number() over (
        partition by gid
        order by updated_at desc, imported_at desc
    ) = 1
),

vitai_paciente_dedup as (
    select gid, nome, cpf
    from {{ ref('raw_prontuario_vitai__paciente') }}
    qualify row_number() over (partition by gid order by updated_at desc) = 1
),

vitai_altas as (
    select
        ep.id_evento_obstetrico,
        ep.subtipo_evento,
        ep.origem_data_alta,
        ep.cpf,
        coalesce(ph.nome_hci, nullif(trim(pac.nome), '')) as nome,
        'Rio de Janeiro' as municipio,
        'RJ' as uf,
        ep.data_parto,
        ep.data_alta_internacao,
        ep.cnes_estabelecimento as cnes_vitai,
        ep.loaded_at as datalake_loaded_at
    from eventos_obstetricos ep
    left join vitai_boletim_dedup b on b.gid = ep.id_hci
    left join vitai_paciente_dedup pac on pac.gid = b.gid_paciente
    left join pacientes_hci ph on ph.cpf = ep.cpf
    where ep.fonte = 'vitai'
      and ep.cpf is not null
),

base_vitai as (
    select
        va.id_evento_obstetrico,
        va.subtipo_evento,
        va.origem_data_alta,
        va.cpf,
        va.nome,
        va.municipio,
        va.uf,
        cast(null as datetime) as data_hora_digitacao,
        va.data_alta_internacao,
        regexp_replace(va.cnes_vitai, r'\D', '') as cnes_maternidade_alta,
        e.nome_maternidade_alta,
        va.data_parto,
        cast(null as int64) as id_desfecho_gestacao,
        cast(null as string) as desfecho_gestacao,
        cg.telefone_cegonha,
        vt.telefone as telefone_vitacare,
        vi.telefone as telefone_vitai,
        'vitai' as prontuario_origem,
        va.datalake_loaded_at
    from vitai_altas va
    left join estabelecimento e
        on e.cnes_maternidade_alta = regexp_replace(va.cnes_vitai, r'\D', '')
    left join cegonha_tel cg on cg.cpf = va.cpf
    left join vitacare_tel vt on vt.cpf = va.cpf
    left join vitai_tel vi on vi.cpf = va.cpf
),

base_unificada as (
    select * from base
    union all
    select * from base_prontuario
    union all
    select * from base_mv
    union all
    select * from base_vitai
),

telefones_explodidos as (

    select
        b.id_evento_obstetrico,
        tel.telefone,
        tel.origem,
        tel.prioridade
    from base_unificada b,
    unnest([
        struct(b.telefone_cegonha    as telefone, 'cegonha'     as origem, 1 as prioridade),
        struct(b.telefone_vitacare   as telefone, 'vitacare'    as origem, 2 as prioridade),
        struct(b.telefone_vitai      as telefone, 'vitai'       as origem, 3 as prioridade)
    ]) as tel
    where {{ normalize_null("trim(tel.telefone)") }} is not null

),

telefones_deduplicados as (

    select *
    from telefones_explodidos
    qualify row_number() over (
        partition by id_evento_obstetrico, regexp_replace(telefone, r'\D', '')
        order by prioridade
    ) = 1

),

telefones_agregados as (

    select
        id_evento_obstetrico,
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
    group by id_evento_obstetrico

),

final as (

    select
        b.id_evento_obstetrico,
        b.subtipo_evento,
        b.origem_data_alta,
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
        b.prontuario_origem,
        b.datalake_loaded_at,
        coalesce(t.telefones_gestante, []) as telefones_gestante
    from base_unificada b
    left join telefones_agregados t
        on t.id_evento_obstetrico = b.id_evento_obstetrico

),

excecao_disparo_puerperas as (

    select
        {{ dbt_utils.generate_surrogate_key([
            "'interno'",
            "cpf",
            "CAST(data_alta_internacao AS STRING)",
            "cnes_maternidade_alta"
        ]) }} as id_evento_obstetrico,
        'excecao_manual' as subtipo_evento,
        'excecao_manual' as origem_data_alta,
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
        'interno' as prontuario_origem,
        cast(null as datetime) as datalake_loaded_at,
        telefones_gestante
    from {{ source("projeto_whatsapp", "excecao_disparo_puerperas") }}

)

select *
from final

union all

select *
from excecao_disparo_puerperas
