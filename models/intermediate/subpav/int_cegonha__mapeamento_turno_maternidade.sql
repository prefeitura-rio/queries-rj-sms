{{ config(
    schema = 'intermediario_cegonha',
    alias = 'mapeamento_turno_maternidade',
    materialized = 'table'
) }}

-- Recupera, para cada id_turnos_horarios, a vaga e o horário associados ao fluxo da agenda estruturada
with uth as (

    select
        id_turnos_horario as id_turnos_horarios,
        any_value(id_unidades_agendamento_vagas) as id_unidades_agendamento_vagas,
        any_value(id_horario) as id_horario
    from {{ ref("raw_plataforma_subpav_cegonha__unidades_turnos_horarios") }}
    where id_turnos_horario is not null
    group by 1

),

-- Recupera o nome do horário oficial da agenda estruturada
horarios as (

    select
        id_horario,
        any_value(nme_horario) as nme_horario
    from {{ ref("raw_plataforma_subpav_cegonha__horarios") }}
    where id_horario is not null
    group by 1

),

-- Relaciona a vaga da agenda ao vínculo de referência e encaminhamento
uav as (

    select
        id_unidades_agendamento_vagas,
        any_value(id_unidades_referencia_encaminha) as id_unidades_referencia_encaminha
    from {{ ref("raw_plataforma_subpav_cegonha__unidades_agendamento_vagas") }}
    where id_unidades_agendamento_vagas is not null
    group by 1

),

-- Recupera, para cada vínculo estrutural, o CNES da maternidade agendada e o CNES da APS de origem
ure as (

    select
        id_unidades_referencia_encaminha,
        any_value(num_cnes_referencia) as cnes_maternidade_agendada,
        any_value(num_cnes_encaminha) as cnes_aps_origem
    from {{ ref("raw_plataforma_subpav_cegonha__unidades_referencia_encaminha") }}
    where id_unidades_referencia_encaminha is not null
    group by 1

)

select
    uth.id_turnos_horarios,
    uth.id_unidades_agendamento_vagas,
    uth.id_horario,
    h.nme_horario,
    uav.id_unidades_referencia_encaminha,
    ure.cnes_maternidade_agendada,
    ure.cnes_aps_origem
from uth
left join horarios h
    on h.id_horario = uth.id_horario
left join uav
    on uav.id_unidades_agendamento_vagas = uth.id_unidades_agendamento_vagas
left join ure
    on ure.id_unidades_referencia_encaminha = uav.id_unidades_referencia_encaminha