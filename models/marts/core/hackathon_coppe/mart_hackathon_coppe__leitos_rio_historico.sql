with
leitos_rio_historico as (
    select
        ano_competencia as ano,
        mes_competencia as mes,
        id_cnes as unidade_id_cnes,

        tipo_leito_descr as leito,
        tipo_especialidade_leito_descr as leito_especialidade,

        quantidade_sus as leito_quantidade_sus,
        quantidade_contratado as leito_quantidade_contratado,
        quantidade_total as leito_quantidade_total

    from {{ ref("dim_leito_sus_rio_historico")}}
    where 1 = 1
        and ano_competencia >= 2022
        and ano_competencia < 2025
)

select * from leitos_rio_historico
where leito is not null
