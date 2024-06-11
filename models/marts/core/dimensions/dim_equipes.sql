{{
    config(
        schema="saude_dados_mestres",
        alias="equipe_profissional_saude",
        materialized="table",
    )
}}
with
    equipes_rj as (
        select
            codigo_unidade,
            sequencial_equipe,
            data_atualizacao,
            array_agg(distinct codigo_profissional_sus) as profissionais
        from {{ ref("raw_cnes_web__equipe_profissionais") }}
        where codigo_municipio = '330455'
        group by 1, 2, 3
    ),
    equipes_rj_ordernado as (
        select
            *,
            row_number() over (
                partition by codigo_unidade, sequencial_equipe
                order by data_atualizacao desc
            ) as ordenacao
        from equipes_rj
    )

select
    equipe.codigo_equipe,
    equipe.sequencial_equipe,
    equipe.codigo_area,
    equipe.nome_referencia as nome_equipe,
    equipe.tipo_equipe as tipo_equipe,
    equipe.codigo_subtipo_equipe as subtipo_equipe,
    equipe.codigo_unidade as codigo_unidade_saude,
    equipe.codigo_profissional_preceptor as codigo_profissional_preceptor,
    lista_profissionais.profissionais,
    equipe.data_atualizacao as ultima_atualizacao_infos_equipe,
    lista_profissionais.data_atualizacao as ultima_atualizacao_profissionais_equipe,
from {{ ref("raw_cnes_web__equipe") }} as equipe

left join
    (select * from equipes_rj_ordernado where ordenacao = 1) as lista_profissionais
    on (
        lista_profissionais.codigo_unidade = equipe.codigo_unidade
        and lista_profissionais.codigo_equipe = equipe.codigo_equipe
    )

inner join
    (
        select distinct id_unidade from {{ ref("dim_estabelecimento") }}
    ) as estabelecimento
    on estabelecimento.id_unidade = equipe.codigo_unidade
where equipe.codigo_municipio = '330455'
