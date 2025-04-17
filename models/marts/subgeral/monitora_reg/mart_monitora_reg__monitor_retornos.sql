{{
    config(
        materialized="incremental",
        schema="projeto_monitora_reg",
        alias="monitor_retornos",
        partition_by={
            "field": "data_retorno",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    marcacoes as (
        select
            date(data_marcacao) as data_marcacao,
            procedimento_interno_id,
            paciente_cpf,
            unidade_executante_id,
            vaga_consumida_tp
        from {{ ref("raw_sisreg_api__marcacoes") }}
        where
            data_marcacao is not null
            and procedimento_interno_id is not null
            and paciente_cpf is not null
            and unidade_executante_id is not null
            and vaga_consumida_tp is not null

            {% if is_incremental() %}
                and date(data_marcacao) >= (
                    select
                        coalesce(
                            max(date(data_retorno)),
                            date_sub(current_date, interval 1 year)
                        )
                    from {{ this }}
                )
            {% endif %}
    ),

    retornos as (
        select
            data_marcacao as data_retorno,
            procedimento_interno_id,
            paciente_cpf,
            unidade_executante_id
        from marcacoes
        where vaga_consumida_tp = "RETORNO"
    ),

    primeira_vez as (
        select
            data_marcacao as data_primeira_vez,
            procedimento_interno_id,
            paciente_cpf,
            unidade_executante_id,
            vaga_consumida_tp
        from marcacoes
        where vaga_consumida_tp in ('1 VEZ', 'RESERVA TECNICA')
    ),

    cruzamento as (
        select
            r.data_retorno,
            p.data_primeira_vez,
            p.vaga_consumida_tp,
            r.procedimento_interno_id as proced_sisreg_id,
            r.paciente_cpf,
            r.unidade_executante_id as unidade_exec_cnes,

            date_diff(
                r.data_retorno, p.data_primeira_vez, day
            ) as dias_entre_primeira_retorno,

            ifnull(
                date_diff(r.data_retorno, p.data_primeira_vez, day) <= 180, false
            ) as primeira_vez_180_dias,

            p.data_primeira_vez is not null as encontrou_primeira_vez

        from retornos r

        left join
            primeira_vez p
            on p.paciente_cpf = r.paciente_cpf
            and p.procedimento_interno_id = r.procedimento_interno_id
            and p.unidade_executante_id = r.unidade_executante_id
            and p.data_primeira_vez <= r.data_retorno

        qualify
            row_number() over (
                partition by
                    r.data_retorno,
                    r.procedimento_interno_id,
                    r.paciente_cpf,
                    r.unidade_executante_id
                order by
                    p.data_primeira_vez desc,
                    case
                        p.vaga_consumida_tp
                        when '1 VEZ'
                        then 1
                        when 'RESERVA TECNICA'
                        then 2
                        else 3
                    end,
                    p.data_primeira_vez
            )
            = 1
    )

select *
from cruzamento
