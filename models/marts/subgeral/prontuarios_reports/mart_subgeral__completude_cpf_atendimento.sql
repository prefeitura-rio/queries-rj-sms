{{
    config(
        enabled=true,
        schema="projeto_prontuarios_reports",
        alias="completude_cpf_atendimentos",
    )
}}

with

    episodios_vitai as (
        select
            bol.gid as id_episodio,
            safe_cast(bol.updated_at as date) as data_episodio,
            pac.cpf as cpf_paciente,
            'vitai' as fornecedor
        from {{ ref("raw_prontuario_vitai__boletim") }} bol
        left join
            {{ ref("raw_prontuario_vitai__paciente") }} pac
            on pac.gid = bol.gid_paciente
        where bol.updated_at <= current_date()
    ),
    episodios_vitacare as (
        select
            ate.id_prontuario_global as id_episodio,
            safe_cast(ate.datahora_fim as date) as data_episodio,
            ate.cpf as cpf_paciente,
            'vitacare' as fornecedor
        from {{ ref("raw_prontuario_vitacare__atendimento") }} ate
        where ate.datahora_fim <= current_date()
    ),
    consolidado as (
        select *
        from episodios_vitai
        union all
        select *
        from episodios_vitacare
    ),
    avaliados as (
        select
            cpf_paciente,
            fornecedor,
            data_episodio,
            if(cpf_paciente is null, 1, 0) as indicador_cpf_nulo,
            cast(
                not {{ validate_cpf("cpf_paciente") }} as int64
            ) as indicador_cpf_invalido
        from consolidado
    ),
    agrupamento_mes_fornecedor as (
        select
            date_trunc(data_episodio, month) as mes_referencia,
            fornecedor,
            count(*) as n_atendimentos,
            sum(indicador_cpf_nulo) as com_cpf_nulo,
            sum(indicador_cpf_invalido) as com_cpf_invalidos,
            round(sum(indicador_cpf_nulo) / count(*) * 100, 2) as perc_cpf_nulo,
            round(sum(indicador_cpf_invalido) / count(*) * 100, 2) as perc_cpf_invalido
        from avaliados
        group by 1, 2
    )

select *
from agrupamento_mes_fornecedor
order by mes_referencia desc, fornecedor asc
