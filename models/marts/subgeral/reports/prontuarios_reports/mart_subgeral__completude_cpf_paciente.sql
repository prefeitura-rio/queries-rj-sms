{{
    config(
        enabled=true,
        schema="projeto_prontuarios_reports",
        alias="completude_cpf_pacientes",
    )
}}

with

    -- Listagem de Pacientes Atendidos e o Mês do Atendimento
    pacientes_atendidos_vitai as (
        select distinct
            bol.gid_paciente as id_paciente,
            date_trunc(safe_cast(bol.updated_at as date), month) as mes_referencia
        from {{ ref("raw_prontuario_vitai__boletim") }} bol
        where bol.updated_at <= current_date()
    ),
    -- Enriquecendo com CPF do Paciente
    pacientes_enriquecidos_vitai as (
        select
            mes_referencia, id_paciente, pac.cpf as cpf_paciente, 'vitai' as fornecedor
        from pacientes_atendidos_vitai
        left join
            {{ ref("raw_prontuario_vitai__paciente") }} pac
            on pac.gid = pacientes_atendidos_vitai.id_paciente
    ),

    -- Listagem de Pacientes Atendidos e o Mês do Atendimento
    -- PS.: Vitacare só temos o CPF
    pacientes_atendidos_vitacare as (
        select distinct
            date_trunc(safe_cast(ate.datahora_fim as date), month) as mes_referencia,
            ate.cpf as id_paciente,
            ate.cpf as cpf_paciente,
            'vitacare' as fornecedor
        from {{ ref("raw_prontuario_vitacare__atendimento") }} ate
        where ate.datahora_fim <= current_date()
    ),

    -- Consolidando Listagem
    consolidado as (
        select *
        from pacientes_enriquecidos_vitai
        union all
        select *
        from pacientes_atendidos_vitacare
    ),
    avaliados as (
        select
            mes_referencia,
            id_paciente,
            cpf_paciente,
            fornecedor,
            if(cpf_paciente is null, 1, 0) as indicador_cpf_nulo,
            cast(
                not {{ validate_cpf("cpf_paciente") }} as int64
            ) as indicador_cpf_invalido
        from consolidado
    ),
    agrupamento_mes_fornecedor as (
        select
            mes_referencia,
            fornecedor,
            count(*) as n_pacientes,
            sum(indicador_cpf_nulo) as com_cpf_nulo,
            sum(indicador_cpf_invalido) as com_cpf_invalidos,
            round(sum(indicador_cpf_nulo) / count(*) * 100, 2) as perc_cpf_nulo,
            round(sum(indicador_cpf_invalido) / count(*) * 100, 2) as perc_cpf_invalido
        from avaliados
        group by 1, 2
        order by fornecedor
    )

select *
from agrupamento_mes_fornecedor
order by mes_referencia desc, fornecedor asc
