-- noqa: disable=LT08
-- Calcula o numero de dias sem resposta para o ultimo evento do paciente relacionado ao monitoramento do cancer de mama
{{
  config(
    enabled=true,
    schema="projeto_monitora_cancer",
    alias="pacientes_dias_sem_resposta",
    unique_key=['paciente_cpf'],
    partition_by={
      "field": "data_ultimo_evento",
      "data_type": "date",
      "granularity": "month",
    },
    cluster_by=['estado_atual', 'paciente_cpf'],
    on_schema_change='sync_all_columns'
  )
}}

with
    fatos as (
        select
            paciente_cpf,
            data_solicitacao,
            data_autorizacao,
            data_execucao,
            data_exame_resultado
        from {{ ref('mart_monitora_cancer__fatos') }}
        where data_solicitacao >= '2025-01-01'
    ),

    explode_datas as (
        select
            paciente_cpf,
            e.event_name,
            e.event_date
        from fatos
            cross join unnest ([
            struct(
                'solicitacao' as event_name,
                data_solicitacao as event_date
            ),

            struct('autorizacao', data_autorizacao),
            struct('execucao', data_execucao),
            struct('exame_resultado', data_exame_resultado)
        ]) as e
        where e.event_date is not null
    ),

    ultimo_evento_usuario as (
        select
            paciente_cpf,
            event_name as estado_atual,
            event_date as data_ultimo_evento,
            date_diff(current_date(), event_date, day) as dias_sem_resposta,
            row_number() over (partition by paciente_cpf order by event_date desc) as rn
        from explode_datas
            qualify rn = 1
    )

select
    paciente_cpf,
    estado_atual,
    data_ultimo_evento,
    dias_sem_resposta
from ultimo_evento_usuario
