{{
    config(
        enabled=true,
        schema="projeto_sisreg_reports",
        alias="oferta_programada__sobreposicao_escalas",
        materialized="incremental",
        partition_by={"field": "data_calculo_anomalia", "data_type": "DATE"},
    )
}}

{% set data_atual = run_started_at.strftime("%Y-%m-%d") %}

with
    particao_mais_recente as (
        select max(data_particao) as data_maxima
        from {{ ref("raw_sisreg__oferta_programada") }}
    ),

    escalas as (
        select
            id_escala_ambulatorial,
            profissional_executante_cpf,
            id_cbo2002,
            id_estabelecimento_executante,
            procedimento_dia_semana_sigla,
            procedimento_vigencia_inicial_data as inicio_datetime,
            procedimento_vigencia_final_data as fim_datetime,

            procedimento_hora_inicial as inicio_time,
            procedimento_hora_final as fim_time

        from {{ ref("raw_sisreg__oferta_programada") }}

        where
            data_particao = (select data_maxima from particao_mais_recente)
            and extract(year from procedimento_vigencia_inicial_data)
            = extract(year from current_date())
            and extract(month from procedimento_vigencia_inicial_data)
            = (extract(month from current_date()) + 1)
    ),

    overlapping as (
        select
            a.id_escala_ambulatorial as a_cod,
            b.id_escala_ambulatorial as b_cod,

            a.profissional_executante_cpf as cpf,
            a.procedimento_dia_semana_sigla as dia_semana,

            a.id_cbo2002 as a_cbo,
            b.id_cbo2002 as b_cbo,

            a.id_estabelecimento_executante as a_cnes,
            b.id_estabelecimento_executante as b_cnes,

            a.inicio_datetime as a_inicio,
            a.inicio_time as a_inicio_time,
            b.inicio_datetime as b_inicio,
            b.inicio_time as b_inicio_time,

            a.fim_datetime as a_fim,
            a.fim_time as a_fim_time,
            b.fim_datetime as b_fim,
            b.fim_time as b_fim_time

        from escalas a

        join
            escalas b

            on a.profissional_executante_cpf = b.profissional_executante_cpf
            and a.procedimento_dia_semana_sigla = b.procedimento_dia_semana_sigla
            and a.id_escala_ambulatorial != b.id_escala_ambulatorial

        where
            a.inicio_datetime <= b.fim_datetime
            and a.fim_datetime >= b.inicio_datetime
            and a.inicio_time < b.fim_time
            and a.fim_time > b.inicio_time
    ),

    final as (
        select
            a_cnes as id_cnes,
            string_agg(
                concat(a_cod, ":", b_cod), ', '
            ) as codigos_escalas_sobrepostas_sisreg,
            date('{{ data_atual }}') as data_calculo_anomalia
        from overlapping
        group by a_cnes
        order by a_cnes
    )

select *
from final
{% if is_incremental() %}
    where data_calculo_anomalia > (select max(data_calculo_anomalia) from {{ this }})
{% endif %}
