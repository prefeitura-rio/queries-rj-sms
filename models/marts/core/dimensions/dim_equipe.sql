{{
    config(
        schema="saude_dados_mestres",
        alias="equipe_profissional_saude",
        tags = ["weekly"],
    )
}}
with
    profissionais_enriquecido as (
        select
            id_unidade as id_unidade_saude,
            equipe_sequencial,
            id_profissional_sus,
            equipe_p.data_particao as ultima_atualizacao_profissionais,
            case
                when upper(descricao) like "MEDIC%"
                then "medicos"
                when
                    (upper(descricao) like "ENFERME%")
                    and (
                        upper(descricao)
                        not like "%SOCORRISTA (EXCETO MEDICOS E ENFERMEIROS)%"
                    )
                then "enfermeiros"
                when
                    (upper(descricao) like "AUXILIAR DE ENFERMAG%")
                    or (upper(descricao) like "TECNICO DE ENFERMAG%")
                then "auxiliares_tecnicos_enfermagem"
                when
                    (upper(descricao) like "AGENTE COMUNI%")
                    or (upper(descricao) like "AGENTE DE%")
                then "agentes_comunitarios"
                when
                    (upper(descricao) like "TECNICO EM SAUDE BUCAL%")
                    or (upper(descricao) like "AUXILIAR EM SAUDE BUCAL%")
                then "auxiliares_tecnico_saude_bucal"
                when
                    (upper(descricao) like "CIRURGIAO DENTISTA%")
                    or (upper(descricao) like "CIRURGIAO-DENTISTA%")
                    or (upper(descricao) like "CIRURGIAODENTISTA%")
                then "dentista"
                -- when (upper(descricao) like "PSIC%")
                -- then "PSICOLOGO"
                -- when (upper(descricao) like "FISIOTERAP%")
                -- then "FISIOTERAPEUTA"
                -- when (upper(descricao) like "NUTRIC%") or (upper(descricao) like
                -- "NUTRIÇ%")
                -- then "NUTRICIONISTA"
                -- when (upper(descricao) like "FONO%")
                -- then "FONOAUDIOLOGO"
                -- when (upper(descricao) like "FARM%")
                -- then "FARMACEUTICO"
                else "outros_profissionais"
            end grupo,
            dense_rank() over (
                partition by id_unidade, equipe_sequencial
                order by equipe_p.data_particao desc
            ) as ordenacao
        from {{ ref("raw_cnes_web__equipe_profissionais") }} as equipe_p
        left join {{ ref("raw_datasus__cbo") }} as cbo on cbo.id_cbo = equipe_p.id_cbo
        where id_municipio = '330455'

    ),
profissionais_equipe as (
select * EXCEPT(ordenacao)
from
    profissionais_enriquecido pivot (
        array_agg(distinct id_profissional_sus ignore nulls)
        for grupo in (
            'medicos',
            'enfermeiros',
            'auxiliares_tecnicos_enfermagem',
            'agentes_comunitarios',
            'auxiliares_tecnico_saude_bucal',
            'dentista',
            'outros_profissionais'
        )
    )
where ordenacao = 1
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
),
contato_equipe as (
    select ine, telefone
    from {{ ref("raw_plataforma_smsrio__equipe_contato") }}
)
select
    equipe.equipe_ine as ine,
    equipe.equipe_nome as nome_referencia,
    equipe.id_unidade as id_unidade_saude,
    equipe.id_tipo_equipe as id_equipe_tipo,
    dim_tipo_equipe.equipe_descricao as equipe_tipo_descricao,
    equipe.id_area,
    dim_area_segmento.area_descricao,
    dim_area_segmento.segmento_descricao,
    contato_equipe.telefone,
    profissionais_equipe.medicos,
    profissionais_equipe.enfermeiros,
    profissionais_equipe.auxiliares_tecnicos_enfermagem,
    profissionais_equipe.agentes_comunitarios,
    profissionais_equipe.auxiliares_tecnico_saude_bucal,
    profissionais_equipe.dentista,
    profissionais_equipe.outros_profissionais,
    profissionais_equipe.ultima_atualizacao_profissionais,
    equipe.data_atualizacao as ultima_atualizacao_infos_equipe
from equipe
inner join
    dim_estabelecimentos on dim_estabelecimentos.id_unidade = equipe.id_unidade
left join
    profissionais_equipe
    on (
        profissionais_equipe.id_unidade_saude = equipe.id_unidade
        and profissionais_equipe.equipe_sequencial = equipe.equipe_sequencial
    )
left join dim_area_segmento on dim_area_segmento.id_area = equipe.id_area
left join dim_tipo_equipe on dim_tipo_equipe.id_equipe_tipo = equipe.id_tipo_equipe
left join contato_equipe on  LPAD(contato_equipe.ine,10,'0') = equipe.equipe_ine
