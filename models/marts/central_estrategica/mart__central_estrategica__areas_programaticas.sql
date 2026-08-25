{{
  config(
    enabled=true,
    alias="areas_programaticas",
    materialized='table',
  )
}}

/*
  Quantidade de pacientes cadastrados com situação ativa por Área Programática (AP).

  Granularidade: um registro por área programática distinta.

  Os pacientes são contados a partir dos registros de cadastro do Vitacare
  (raw_prontuario_vitacare_historico__cadastro), filtrando apenas cadastros
  permanentes com situação ativa. A Área Programática é resolvida via
  dim_estabelecimento a partir do CNES da unidade de saúde do cadastro,
  seguindo o mesmo padrão dos demais modelos central_estrategica.

  Fonte: raw_prontuario_vitacare_historico__cadastro + dim_estabelecimento
*/

with
    -- Cadastros ativos e permanentes no Vitacare
    cadastros_ativos as (
        select
            c.id_cnes
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }} as c
        where
            c.situacao_usuario = 'Ativo'
            and c.cadastro_permanente = true
    ),

    -- Área programática por CNES via dim_estabelecimento
    estabelecimento as (
        select
            d.id_cnes,
            d.area_programatica
        from {{ ref('dim_estabelecimento') }} as d
    ),

    -- Associa cada cadastro à sua área programática
    cadastros_com_ap as (
        select
            est.area_programatica
        from cadastros_ativos as c
        inner join estabelecimento as est
            on est.id_cnes = c.id_cnes
        where est.area_programatica is not null
    ),

    final as (
        select
            area_programatica,
            count(*) as total_pacientes_ativos
        from cadastros_com_ap
        group by area_programatica
    )

select * from final
order by area_programatica asc
