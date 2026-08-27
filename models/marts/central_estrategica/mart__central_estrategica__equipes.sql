{{
  config(
    enabled=true,
    alias="equipes",
    materialized='table',
  )
}}

/*
  Catálogo de equipes de saúde com atividade registrada no Vitacare,
  enriquecido com metadados cadastrais provenientes de dim_equipe.

  Granularidade: um registro por equipe (cod_ine_equipe_profissional distinto).

  A data do primeiro e do último atendimento são derivadas dos registros de
  atendimento da Vitacare (raw_prontuario_vitacare__atendimento).
  O nome e o CNES da unidade são extraídos do nome mais frequente registrado
  nos atendimentos; dados de tipo, área e contato vêm de dim_equipe quando
  o INE é localizado na dimensão.

  Fonte: raw_prontuario_vitacare__atendimento + dim_equipe
*/

with
    -- Atendimentos vitacare com equipe preenchida
    atendimentos as (
        select
            a.cod_ine_equipe_profissional    as ine,
            a.nome_equipe_profissional       as nome_equipe,
            a.cnes_unidade,
            date(a.datahora_inicio)          as data_atendimento
        from {{ ref('raw_prontuario_vitacare__atendimento') }} as a
        where
            a.cod_ine_equipe_profissional is not null
            and a.cod_ine_equipe_profissional != ''
            and a.datahora_inicio is not null
    ),

    -- Agrega por INE: datas extremas e total de atendimentos
    agregado as (
        select
            ine,
            min(data_atendimento)   as data_primeiro_atendimento,
            max(data_atendimento)   as data_ultimo_atendimento,
            count(*)                as total_atendimentos
        from atendimentos
        group by ine
    ),

    -- Nome de equipe mais frequente por INE
    nome_mais_frequente as (
        select
            ine,
            nome_equipe
        from (
            select
                ine,
                nome_equipe,
                row_number() over (
                    partition by ine
                    order by count(*) desc, nome_equipe
                ) as ordenacao
            from atendimentos
            where nome_equipe is not null and nome_equipe != ''
            group by ine, nome_equipe
        )
        where ordenacao = 1
    ),

    -- CNES mais frequente por INE
    cnes_mais_frequente as (
        select
            ine,
            cnes_unidade
        from (
            select
                ine,
                cnes_unidade,
                row_number() over (
                    partition by ine
                    order by count(*) desc, cnes_unidade
                ) as ordenacao
            from atendimentos
            where cnes_unidade is not null and cnes_unidade != ''
            group by ine, cnes_unidade
        )
        where ordenacao = 1
    ),

    -- Dimensão de equipes (CNES oficial)
    dim as (
        select
            e.id_ine,
            e.id_cnes,
            e.nome_referencia,
            e.equipe_tipo_descricao,
            e.area_descricao,
            e.telefone
        from {{ ref('dim_equipe') }} as e
    ),

    -- Cadastros ativos por equipe (INE)
    pacientes_por_equipe as (
        select
            c.ine_equipe,
            count(*) as total_pacientes_ativos
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }} as c
        where
            c.situacao_usuario = 'Ativo'
            and c.cadastro_permanente = true
            and c.ine_equipe is not null
        group by c.ine_equipe
    ),

    -- Área programática por CNES via dim_estabelecimento
    estabelecimento as (
        select
            d.id_cnes,
            d.area_programatica
        from {{ ref('dim_estabelecimento') }} as d
    ),

    final as (
        select
            ag.ine,
            coalesce(dim.nome_referencia, nm.nome_equipe)   as nome_equipe,
            coalesce(dim.id_cnes, cn.cnes_unidade)          as cnes_unidade,
            est.area_programatica,
            split(dim.equipe_tipo_descricao, ' - ')[offset(0)]  as tipo_equipe,
            dim.area_descricao                              as area,
            dim.telefone,
            ag.data_primeiro_atendimento,
            ag.data_ultimo_atendimento,
            ag.total_atendimentos,
            coalesce(pq.total_pacientes_ativos, 0)          as total_pacientes_ativos
        from agregado as ag
        left join nome_mais_frequente as nm
            on nm.ine = ag.ine
        left join cnes_mais_frequente as cn
            on cn.ine = ag.ine
        left join dim
            on dim.id_ine = ag.ine
        left join estabelecimento as est
            on est.id_cnes = coalesce(dim.id_cnes, cn.cnes_unidade)
        left join pacientes_por_equipe as pq
            on pq.ine_equipe = ag.ine
    )

select * from final
order by total_atendimentos desc
