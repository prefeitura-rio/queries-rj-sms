{{ config(
    schema = 'projeto_whatsapp',
    alias = 'cegonha_agendamento_maternidade',
    materialized = 'table',
    cluster_by = ['cpf', 'cnes_maternidade_agendada']
) }}

-- Cria um ranking por id_agendamento_gestante para escolher o registro mais recente
with agendamento_rank as (

    select
        cast(id_agendamento_gestante as string) as id_agendamento_gestante,
        cast(id_gestante as string) as id_gestante,
        cast(id_turnos_horarios as string) as id_turnos_horarios,
        cast(dta_visita_maternidade as date) as data_agendamento,
        cast(tel_contato as string) as telefone_cegonha,
        cast(nome_acompanhante as string) as nome_acompanhante,
        cast(tel_contato_acompanhante as string) as telefone_acompanhante,
        row_number() over (
            partition by cast(id_agendamento_gestante as string)
            order by
                cast(dta_visita_maternidade as datetime) desc,
                cast(created_at as datetime) desc
        ) as rn
    from {{ ref('raw_plataforma_subpav_cegonha__agendamento_gestantes') }}
    where id_agendamento_gestante is not null
      and id_gestante is not null

),

-- Mantém apenas a linha mais recente de cada agendamento
agendamento_base as (

    select
        id_agendamento_gestante,
        id_gestante,
        id_turnos_horarios,
        data_agendamento,
        telefone_cegonha,
        nome_acompanhante,
        telefone_acompanhante
    from agendamento_rank
    where rn = 1

),

-- Dados complementares do agendamento via fluxo manual, utilizados quando não há informações disponíveis na agenda estruturada
visita_base as (

    select
        cast(id_agendamento_gestante as string) as id_agendamento_gestante,
        num_cnes_atendimento as cnes_maternidade_agendada_manual,
        nme_horario_padronizado as horario_visita
    from {{ ref('raw_plataforma_subpav_cegonha__visita_gestantes_tipos') }}

),

-- Recupera informacoes da gestante ja enriquecidas com cpf de outras bases a partir do modelo intermediário
gestantes as (

    select
        cast(id_gestante as string) as id_gestante,
        nme_nome as nome,
        cpf
    from {{ ref('int_cegonha__gestantes') }}
    where id_gestante is not null

),

-- Recupera, no fluxo estruturado, a maternidade agendada e o horário do turno para cada id_turnos_horarios
mapeamento_turno as (

    select
        cast(id_turnos_horarios as string) as id_turnos_horarios,
        cnes_maternidade_agendada,
        nme_horario as horario_turno
    from {{ ref('int_cegonha__mapeamento_turno_maternidade') }}

),

-- Resgata o nome da unidade em que foi agendado através do cnes
estabelecimento as (

    select
        cast(id_cnes as string) as cnes,
        any_value(nome_fantasia) as nome_maternidade_agendada
    from {{ ref('raw_gdb_cnes__estabelecimento') }}
    where id_cnes is not null
    group by 1

),

-- Consolida um telefone da paciente por CPF a partir de diferentes origens
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

-- Monta a base principal do mart
-- Resolve a maternidade agendada conforme o fluxo do registro: se existir id_turnos_horarios, usa o fluxo estruturado de agenda para obter o CNES da maternidade e o horário do turno
-- senão, usa o fluxo manual via visita_gestantes_tipos para obter o CNES da maternidade e o horário da visita
base as (

    select
        a.id_agendamento_gestante,
        g.nome,
        g.cpf,
        case
            when a.id_turnos_horarios is not null then mt.cnes_maternidade_agendada
            else vb.cnes_maternidade_agendada_manual
        end as cnes_maternidade_agendada,
        e.nome_maternidade_agendada,
        case
            when a.data_agendamento is not null
             and (
                case
                    when a.id_turnos_horarios is not null then mt.horario_turno
                    else vb.horario_visita
                end
             ) is not null
                then datetime(
                    a.data_agendamento,
                    parse_time(
                        '%H:%M',
                        case
                            when a.id_turnos_horarios is not null then mt.horario_turno
                            else vb.horario_visita
                        end
                    )
                )
            when a.data_agendamento is not null
                then datetime(a.data_agendamento)
            else null
        end as data_hora_agendamento,
        a.nome_acompanhante,
        a.telefone_acompanhante,
        a.telefone_cegonha,
        vt.telefone as telefone_vitacare,
        vi.telefone as telefone_vitai
    from agendamento_base a
    left join gestantes g
        on g.id_gestante = a.id_gestante
    left join mapeamento_turno mt
        on mt.id_turnos_horarios = a.id_turnos_horarios
    left join visita_base vb
        on vb.id_agendamento_gestante = a.id_agendamento_gestante
    left join estabelecimento e
        on e.cnes = case
            when a.id_turnos_horarios is not null then mt.cnes_maternidade_agendada
            else vb.cnes_maternidade_agendada_manual
        end
    left join vitacare_tel vt
        on vt.cpf = g.cpf
    left join vitai_tel vi
        on vi.cpf = g.cpf

),

-- Define prioridade aos numeros de telefone dependendo da origem
telefones_explodidos as (

    select
        b.id_agendamento_gestante,
        b.nome,
        b.cpf,
        b.cnes_maternidade_agendada,
        b.nome_maternidade_agendada,
        b.data_hora_agendamento,
        b.nome_acompanhante,
        b.telefone_acompanhante,
        tel.telefone,
        tel.origem,
        tel.prioridade
    from base b,
    unnest([
        struct(b.telefone_cegonha as telefone, 'cegonha' as origem, 1 as prioridade),
        struct(b.telefone_vitacare as telefone, 'vitacare' as origem, 2 as prioridade),
        struct(b.telefone_vitai as telefone, 'vitai' as origem, 3 as prioridade)
    ]) as tel
    where {{ normalize_null("trim(tel.telefone)") }} is not null

),

-- Remove telefones repetidos dentro do mesmo agendamento, mantendo a origem de maior prioridade
telefones_deduplicados as (

    select
        *,
        {{ padroniza_telefone_whatsapp('telefone') }}.telefone_valido_whatsapp as telefone_valido_whatsapp,
        {{ padroniza_telefone_whatsapp('telefone') }}.motivo_invalidacao_telefone as motivo_invalidacao_telefone
    from telefones_explodidos
    qualify
        telefone_valido_whatsapp is not null
        and row_number() over (
            partition by id_agendamento_gestante, telefone_valido_whatsapp
            order by prioridade
        ) = 1

),

final as (

    select
        id_agendamento_gestante,
        nome,
        cpf,
        cnes_maternidade_agendada,
        nome_maternidade_agendada,
        data_hora_agendamento,
        array_agg(
            struct(
                telefone,
                origem,
                cast(prioridade as string) as prioridade,
                telefone_valido_whatsapp,
                motivo_invalidacao_telefone
            )
            order by prioridade
        ) as telefones,
        nome_acompanhante,
        telefone_acompanhante
    from telefones_deduplicados
    group by
        id_agendamento_gestante,
        nome,
        cpf,
        cnes_maternidade_agendada,
        nome_maternidade_agendada,
        data_hora_agendamento,
        nome_acompanhante,
        telefone_acompanhante

)

select *
from final