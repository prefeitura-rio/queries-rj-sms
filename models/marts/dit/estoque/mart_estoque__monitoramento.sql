{{
    config(
        alias="estoque_monitoramento",
        schema="projeto_estoque",
        materialized="table",
    )
}}


with

    estabelecimento as (
        select id_cnes, area_programatica, nome_limpo, prontuario_versao,
        from {{ ref("dim_estabelecimento") }}
        where
            prontuario_versao in ("vitacare")  -- - "vitai")
            and prontuario_estoque_tem_dado = "sim"
    ),

    -- métrica de dispensação
    aps_atendimento_por_dia as (select * from {{ ref("apv_atendimento_dia") }}),

    dispensacao_por_cpf_dia as (
        select
            id_cnes,
            data_particao,
            count(distinct consumo_paciente_cpf) as cpfs_atendidos_dia
        from {{ ref("fct_estoque_movimento") }}
        group by 1, 2
        order by 1, 2
    ),

    dispensacao_por_cpf_media_diaria as (
        select id_cnes, avg(cpfs_atendidos_dia) as dispensacoes_por_cpf_dia
        from dispensacao_por_cpf_dia
        group by 1
    ),

    metrica_dispensacao as (
        select
            a.id_cnes,
            a.atendimento_dia as atendimentos_por_dia,
            b.dispensacoes_por_cpf_dia,
            {{
                dbt_utils.safe_divide(
                    "b.dispensacoes_por_cpf_dia", "a.atendimento_dia"
                )
            }} as metrica_proporcao_atendimentos_com_dispensacao_por_cpf
        from aps_atendimento_por_dia as a
        left join dispensacao_por_cpf_media_diaria as b using (id_cnes)
    ),

    -- metrica de cadastro
    materiais_por_unidade as (
        select
            id_cnes,
            count(distinct id_material) as material_qtd_distintos,
            count(distinct case when material_quantidade > 0 then id_material else "" end) as material_qtd_distintos_com_saldo,
            count(
                distinct case
                    when material_cadastro_esta_correto = 'nao' then material_descricao
                end
            ) as material_qtd_distintos_cadastro_incorreto,
        from {{ ref("mart_estoque__posicao_atual") }}
        group by 1
    ),

    metrica_cadastro as (
        select
            *,
            1
            -{{
                dbt_utils.safe_divide(
                    "material_qtd_distintos_cadastro_incorreto",
                    "material_qtd_distintos_com_saldo",
                )
            }} as metrica_proporcao_materiais_cadastro_correto
        from materiais_por_unidade
    )

select
    e.id_cnes,
    e.nome_limpo,
    e.area_programatica,
    d.atendimentos_por_dia,
    d.dispensacoes_por_cpf_dia,
    d.metrica_proporcao_atendimentos_com_dispensacao_por_cpf,
    c.material_qtd_distintos,
    c.material_qtd_distintos_com_saldo,
    c.material_qtd_distintos_cadastro_incorreto,
    c.metrica_proporcao_materiais_cadastro_correto,
    current_date('America/Sao_Paulo') as data_referencia
from estabelecimento as e
left join metrica_dispensacao as d using (id_cnes)
left join metrica_cadastro as c using (id_cnes)
order by area_programatica, nome_limpo
