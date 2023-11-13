{{
    config(
        alias="estoque_monitoramento",
        schema="dashboard_estoque",
        materialized="table",

    )
}}


with
    estabelecimento as (
        select
            id_cnes,
            area_programatica,
            nome_limpo,
            prontuario_versao,
        from {{ ref("dim_estabelecimento") }}
        where prontuario_versao in ("vitai", "vitacare") and prontuario_estoque_tem_dado = "sim"
    ),

    dispensacao_media as (
        select
            id_cnes,
            sum(material_valor_total) / count(distinct data_particao) as dispensacao_valor_medio_diario,
            count(id_material) / count(distinct data_particao) as dispensacao_quantidade_media_diario,
        from {{ ref("mart_estoque__movimento") }}
        where
            movimento_tipo_grupo = 'Consumo'
            -- and data_particao >= date_sub(current_date(), interval 8 day)  -- movimento de consumo dos últimos 7 dias
        group by 1
    ),

    metricas as (
        select
            id_cnes,
            estabelecimento_area_programatica,
            estabelecimento_nome_limpo,
            sistema_origem,
            sum(material_valor_total) as material_valor_total,
            max(data_particao) as data_ultima_atualizacao,
            avg(dias_desde_ultima_atualizacao) as dias_desde_ultima_atualizacao,
            count(distinct id_material) as material_qtd_distintos,
            count(
                distinct case
                    when material_cadastro_esta_correto = 'nao' then id_material
                end
            ) as material_qtd_distintos_cadastro_incorreto,
            count(
                distinct case when abc_categoria = 'S/C' then id_material end
            ) as material_qtd_distintos_sem_abc,
            count(
                distinct case when material_valor_unitario = 0 then id_material end
            ) as material_qtd_distintos_sem_valor_unitario,
        from {{ ref("mart_estoque__posicao_atual") }}
        group by 1, 2, 3, 4
    )




select
    coalesce(est.id_cnes, m.id_cnes) as id_cnes,
    coalesce(
        est.area_programatica, m.estabelecimento_area_programatica
    ) as estabelecimento_area_programatica,
    coalesce(est.nome_limpo, m.estabelecimento_nome_limpo) as estabelecimento_nome_limpo,
    coalesce(est.prontuario_versao, m.sistema_origem) as estabelecimento_prontuario_versao,
    if(m.material_valor_total is null, 0, m.material_valor_total) as material_valor_total,
    data_ultima_atualizacao,
    if(dias_desde_ultima_atualizacao is null, 99, dias_desde_ultima_atualizacao) as dias_desde_ultima_atualizacao,
    if(material_qtd_distintos is null, 0, material_qtd_distintos) as material_qtd_distintos,
    if(material_qtd_distintos_cadastro_incorreto is null, 0, material_qtd_distintos_cadastro_incorreto) as material_qtd_distintos_cadastro_incorreto,
    if(material_qtd_distintos_sem_abc is null, 0, material_qtd_distintos_sem_abc) as material_qtd_distintos_sem_abc,
    if(material_qtd_distintos_sem_valor_unitario is null, 0, material_qtd_distintos_sem_valor_unitario) as material_qtd_distintos_sem_valor_unitario,
    dm.dispensacao_valor_medio_diario,
    dm.dispensacao_quantidade_media_diario,
    if(
        m.dias_desde_ultima_atualizacao = 0, "atualizado", "desatualizado"
    ) as status_replicacao_dados
from estabelecimento as est
full outer join metricas as m using (id_cnes)
left join dispensacao_media as dm using (id_cnes)
order by est.nome_limpo desc

