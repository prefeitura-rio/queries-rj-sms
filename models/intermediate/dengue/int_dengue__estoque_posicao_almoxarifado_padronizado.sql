-- - Padroniza os inputs do estoque posição de almoxarifado das APS
with
    dim_material as (select * from {{ ref("dim_material") }}),

    dim_estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    data_mais_recente as (
        select * from {{ ref("int_dengue__estoque_posicao_mais_recente_por_cnes") }}
    ),

    estoque as (
        select *
        from {{ ref("raw_plataforma_smsrio__estoque_posicao_almoxarifado_aps_dengue") }}
    ),

    estoque_mais_recentes as (
        select e.*
        from estoque as e
        inner join
            data_mais_recente as d
            on e.id_cnes = d.id_cnes
            and e.data = d.data
    ),

    materiais as (
        select
            id_material,
            case
                id_material
                when "1"  -- Equipo, soro macrogotas, injetor lateral
                then "65151800400"
                when "2"  -- Jelco 18
                then "65151401467"
                when "3"  -- Jelco 20
                then "65151401548"
                when "4"  -- Jelco 22
                then '65151401629'
                when "5"  -- Scalp 21
                then '65153700879'
                when "6"  -- Scalp 23
                then '65153700950'
                when "7"  -- Scalp 25
                then '65153701093'
                when "8"  -- Tubo para coleta de sangue EDTA (tampa roxa)
                then ''
                when "9"  -- Agulha hipodérmica de seguranca 25X8
                then '65150309109'
                when "10"  -- Seringa descartável 5ML, com agulha 25X8MM
                then '65153802108'
                when "11"  -- Adaptador vacuo
                then ''
                when "12"  -- Luva
                then ''
                when "13"  -- Garrote
                then '65153501833'
                when "14"  -- Caixa descartável
                then ''
            end as id_material_sigma,
            upper(descricao) as material_descricao,
        from {{ ref("raw_plataforma_smsrio__materiais_almoxarifado_dengue") }}
    ),

    materiais_padronizados as (
        select m.*, coalesce(d.nome, m.material_descricao) as material_nome_padronizado
        from materiais as m
        left join dim_material as d on m.id_material_sigma = d.id_material
        order by id_material
    )

select
    estoque.id_cnes,
    m.id_material_sigma as id_material,
    e.agrupador_sms as estabelecimento_agrupador_sms,
    e.area_programatica as estabelecimento_area_programatica,
    e.tipo_sms as estabelecimento_tipo_sms,
    e.nome_limpo as estabelecimento_nome_limpo,
    e.nome_sigla as estabelecimento_nome_sigla,
    m.material_nome_padronizado as material_descricao,
    estoque.material_quantidade,
from estoque_mais_recentes as estoque
left join dim_estabelecimento as e on estoque.id_cnes = e.id_cnes
left join materiais_padronizados as m on estoque.id_material = m.id_material
