with
versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
), 

estabelecimentos_mrj_sus as (
    select distinct safe_cast(id_cnes as int64) as id_cnes from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

leitos_mrj_sus_non_unique as (
    select
        ano as ano_competencia,
        mes as mes_competencia,
        id_estabelecimento_cnes as id_cnes,
        tipo_leito,
        tipo_especialidade_leito,
        quantidade_total,
        quantidade_contratado,
        quantidade_sus

    from
        {{ ref("raw_cnes_ftp__leito") }} 

    where ano >= 2010 and safe_cast(id_estabelecimento_cnes as int64) in (select id_cnes from estabelecimentos_mrj_sus)
)

select distinct * from leitos_mrj_sus_non_unique