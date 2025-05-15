{{
    config(
        enabled=true,
        schema="projeto_subgeral",
        alias="completude_cpf_pacientes",
    )
}}

with

    pacientes_atendidos_vitai as (
        SELECT
            distinct 
            bol.gid_paciente as id_paciente,
            date_trunc(safe_cast(bol.updated_at as date), month) as mes_referencia
        from {{ref("raw_prontuario_vitai__boletim")}} bol
        where bol.updated_at <= current_date()
    ),
    pacientes_enriquecidos_vitai as (
        select
            mes_referencia,
            id_paciente,
            pac.cpf as cpf_paciente,
            'vitai' as fornecedor
        from pacientes_atendidos_vitai
            left join {{ref("raw_prontuario_vitai__paciente")}} pac 
                on pac.gid = pacientes_atendidos_vitai.id_paciente
    ),

    pacientes_atendidos_vitacare as (
        select
            distinct 
            ate.cpf as id_paciente,
            ate.cpf as cpf_paciente,
            date_trunc(safe_cast(ate.datahora_inicio as date), month) as mes_referencia
        from {{ref("raw_prontuario_vitacare__atendimento")}} ate
        where ate.datahora_inicio <= current_date()
    ),
    pacientes_enriquecidos_vitacare as (
        select
            mes_referencia,
            id_paciente,
            cpf_paciente,
            'vitacare' as fornecedor
        from pacientes_atendidos_vitacare
            left join {{ref("raw_prontuario_vitacare__paciente")}} pac 
                on pac.cpf = pacientes_atendidos_vitacare.id_paciente
    ),
    consolidado as (
        select * from pacientes_enriquecidos_vitai
        union all
        select * from pacientes_enriquecidos_vitacare
    ),
    avaliados as (
        select
            mes_referencia,
            id_paciente,
            cpf_paciente,
            fornecedor,
            if(cpf_paciente is null, 1, 0) as indicador_cpf_nulo,
            cast(not {{ validate_cpf("cpf_paciente") }} as int64) as indicador_cpf_invalido
        from consolidado
    ),
    agrupamento_mes_fornecedor as (
        select
            fornecedor,
            mes_referencia,
            sum(indicador_cpf_nulo) as total_cpf_nulo,
            sum(indicador_cpf_invalido) as total_cpf_invalido,
            count(*) as total
        from avaliados
        group by 1,2
        order by fornecedor
    ),
    agrupamento_mes as (
        select
            mes_referencia,
            array_agg(
                struct(
                    fornecedor,
                    total_cpf_nulo as com_cpf_nulo,
                    total_cpf_invalido as com_cpf_invalidos,
                    total
                )
            ) as quantidades_de_pacientes
        from agrupamento_mes_fornecedor
        group by 1
    )
select *
from agrupamento_mes
order by mes_referencia desc
    
    
    
