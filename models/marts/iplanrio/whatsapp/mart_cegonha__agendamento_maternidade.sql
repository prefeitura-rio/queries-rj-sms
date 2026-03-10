{{ config(
    schema = 'projeto_whatsapp',
    alias = 'cegonha_agendamento_maternidade',
    materialized = 'table',
    partition_by = {
        "field": "data_hora_agendamento",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by = ['cpf', 'cnes_maternidade_agendada']
) }}

-- Cria um ranking por id_agendamento_gestante para escolher o registro mais recente
with agendamento_rank as (

    select
        cast(id_agendamento_gestante as string) as id_agendamento_gestante,
        cast(id_gestante as string) as id_gestante,
        cast(id_turnos_horarios as string) as id_turnos_horarios,
        cast(dta_visita_maternidade as date) as data_agendamento,
        nullif(trim(cast(tel_contato as string)), '') as telefone_cegonha,
        nullif(trim(cast(nome_acompanhante as string)), '') as nome_acompanhante,
        nullif(trim(cast(tel_contato_acompanhante as string)), '') as telefone_acompanhante,
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

-- Dados complementares de agendamento atraves do fluxo manual de agendamento, caso nao tenha informacoes atraves do fluxo de agenda
visita_rank as (

    select
        cast(id_agendamento_gestante as string) as id_agendamento_gestante,
        nullif(trim(cast(num_cnes_atendimento as string)), '') as cnes_maternidade_agendada_manual,
        nme_horario_padronizado as horario_visita,
        row_number() over (
            partition by cast(id_agendamento_gestante as string)
            order by id_visita_gestante_tipo
        ) as rn
    from {{ ref('raw_plataforma_subpav_cegonha__visita_gestantes_tipos') }}

),

-- Mantém somente um registro de visita por id_agendamento_gestante
visita_base as (

    select
        id_agendamento_gestante,
        cnes_maternidade_agendada_manual,
        horario_visita
    from visita_rank
    where rn = 1

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

-- Mapeia id_turnos_horarios para os identificadores de vaga e horário da agenda estruturada
uth as (

    select
        cast(id_turnos_horario as string) as id_turnos_horarios,
        any_value(id_unidades_agendamento_vagas) as id_unidades_agendamento_vagas,
        any_value(id_horario) as id_horario
    from {{ ref('raw_plataforma_subpav_cegonha__unidades_turnos_horarios') }}
    where id_turnos_horario is not null
    group by 1

),

-- Recupera o horário do turno a partir do identificador de horário
horarios as (

    select
        id_horario,
        any_value(nme_horario) as horario_turno
    from {{ ref('raw_plataforma_subpav_cegonha__horarios') }}
    where id_horario is not null
    group by 1

),

-- Relaciona a vaga da agenda ao vínculo de referência/encaminhamento
uav as (

    select
        id_unidades_agendamento_vagas,
        any_value(id_unidades_referencia_encaminha) as id_unidades_referencia_encaminha
    from {{ ref('raw_plataforma_subpav_cegonha__unidades_agendamento_vagas') }}
    where id_unidades_agendamento_vagas is not null
    group by 1

),

-- Recupera o CNES da maternidade agendada no fluxo estruturado a partir da referência da vaga
ure as (

    select
        id_unidades_referencia_encaminha,
        any_value(regexp_replace(cast(num_cnes_referencia as string), r'\.0$', '')) as cnes_maternidade_agendada
    from {{ ref('raw_plataforma_subpav_cegonha__unidades_referencia_encaminha') }}
    where id_unidades_referencia_encaminha is not null
    group by 1

),

-- Resolve, no fluxo estruturado, a maternidade agendada e o horário do turno para cada id_turnos_horarios.
mapeamento_turno as (

    select
        uth.id_turnos_horarios,
        ure.cnes_maternidade_agendada,
        h.horario_turno
    from uth
    left join horarios h
        on h.id_horario = uth.id_horario
    left join uav
        on uav.id_unidades_agendamento_vagas = uth.id_unidades_agendamento_vagas
    left join ure
        on ure.id_unidades_referencia_encaminha = uav.id_unidades_referencia_encaminha

),

-- Resgata o nome da unidade em que foi agendado através do cnes
estabelecimento as (

    select
        regexp_replace(cast(id_cnes as string), r'\.0$', '') as cnes,
        any_value(nome_fantasia) as nome_maternidade_agendada
    from {{ ref('raw_gdb_cnes__estabelecimento') }}
    where id_cnes is not null
    group by 1

),

-- Consolida um telefone da paciente por CPF a partir de diferentes origens
vitacare_tel as (

    select
        regexp_replace(cast(cpf as string), r'\D', '') as cpf,
        any_value(nullif(trim(cast(telefone as string)), '')) as telefone
    from {{ ref('raw_prontuario_vitacare__paciente') }}
    where cpf is not null
      and trim(cast(cpf as string)) <> ''
      and telefone is not null
      and trim(cast(telefone as string)) <> ''
      and lower(trim(cast(telefone as string))) not in ('none', 'nan')
    group by 1

),

vitai_tel as (

    select
        regexp_replace(cast(cpf as string), r'\D', '') as cpf,
        any_value(nullif(trim(cast(telefone as string)), '')) as telefone
    from {{ ref('raw_prontuario_vitai__paciente') }}
    where cpf is not null
      and trim(cast(cpf as string)) <> ''
      and telefone is not null
      and trim(cast(telefone as string)) <> ''
      and lower(trim(cast(telefone as string))) not in ('none', 'nan')
    group by 1

),

base as (

    select
        a.id_agendamento_gestante,
        a.id_gestante,
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
        b.id_gestante,
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
    where tel.telefone is not null
      and trim(tel.telefone) <> ''
      and lower(trim(tel.telefone)) not in ('none', 'nan')

),

-- Remove telefones repetidos dentro do mesmo agendamento, mantendo a origem de maior prioridade
telefones_deduplicados as (

    select *
    from telefones_explodidos
    qualify row_number() over (
        partition by id_agendamento_gestante, regexp_replace(telefone, r'\D', '')
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
                {{ padroniza_telefone_whatsapp('telefone') }}.telefone_valido_whatsapp as telefone_valido_whatsapp,
                {{ padroniza_telefone_whatsapp('telefone') }}.motivo_invalidacao_telefone as motivo_invalidacao_telefone
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