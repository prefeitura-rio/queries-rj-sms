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
            id_unidade,
            equipe_sequencial,
            data_atualizacao,
            array_agg(distinct id_profissional_sus) as profissionais
        from {{ ref("raw_cnes_web__equipe_profissionais") }}
        where id_municipio = '330455'
        group by 1, 2, 3
    ),
    equipes_rj_ordernado as (
        select
            *,
            row_number() over (
                partition by id_unidade, equipe_sequencial
                order by data_atualizacao desc
            ) as ordenacao
        from equipes_rj
    ),
    dim_estabelecimentos as (
        select distinct id_unidade from {{ ref("dim_estabelecimento") }}
    ),
    versao_atual as (
        select max(data_particao) as versao from {{ ref("raw_cnes_web__equipe") }}
    ),
    equipe as (
        select *
        from {{ ref("raw_cnes_web__equipe") }}
        where
            id_municipio = '330455'
            and data_particao = (select versao from versao_atual)
    ),
    dim_segmento as (
        select id_segmento, segmento_descricao,
        from {{ ref("raw_cnes_web__segmento") }}
        where
            data_particao = (select versao from versao_atual)
            and id_municipio = '330455'
    ),
    dim_area_segmento as (
        select area.id_area, area.area_descricao, dim_segmento.segmento_descricao
        from {{ ref("raw_cnes_web__area") }} as area
        left join dim_segmento on dim_segmento.id_segmento = area.id_segmento
        where
            area.data_particao = (select versao from versao_atual)
            and area.id_municipio = '330455'
    ),
    dim_tipo_equipe as (
        select id_equipe_tipo, equipe_descricao, id_equipe_grupo
        from {{ ref("raw_cnes_web__equipe_tipo") }}
        where data_particao = (select versao from versao_atual)
    )
select
    equipe.id_equipe,
    equipe.equipe_sequencial,
    equipe.id_area,
    dim_area_segmento.area_descricao,
    dim_area_segmento.segmento_descricao,
    equipe.equipe_nome as nome,
    equipe.id_tipo_equipe as id_tipo_equipe,
    dim_tipo_equipe.equipe_descricao as tipo_equipe_descricao,
    equipe.id_unidade as id_unidade_saude,
    lista_profissionais.profissionais,
    equipe.data_atualizacao as ultima_atualizacao_infos_equipe,
    lista_profissionais.data_atualizacao as ultima_atualizacao_profissionais_equipe,
from equipe
inner join
    dim_estabelecimentos on dim_estabelecimentos.id_unidade = equipe.id_unidade
left join
    (select * from equipes_rj_ordernado where ordenacao = 1) as lista_profissionais
    on (
        lista_profissionais.id_unidade = equipe.id_unidade
        and lista_profissionais.equipe_sequencial = equipe.equipe_sequencial
    )
left join dim_area_segmento on dim_area_segmento.id_area = equipe.id_area
left join dim_tipo_equipe on dim_tipo_equipe.id_equipe_tipo = equipe.id_tipo_equipe

where equipe.id_municipio = '330455'
