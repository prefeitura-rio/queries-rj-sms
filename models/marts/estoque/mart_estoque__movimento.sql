{{
    config(
        alias="estoque_movimento",
        schema="projeto_estoque",
    )
}}

with
    movimento as (select * from {{ ref("fct_estoque_movimento") }}),

    estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),
    
    
    movimento_final as ( 
        select
            mov.*,
            if(
                sistema_origem <> "tpc", est.tipo, "ESTOQUE CENTRAL"    # TODO: adicionar sufixo _cnes
            ) as estabelecimento_tipo,
            if(
                sistema_origem <> "tpc", est.tipo_sms, "ESTOQUE CENTRAL"
            ) as estabelecimento_tipo_sms,
            if(
                sistema_origem <> "tpc", est.area_programatica, "-"
            ) as estabelecimento_area_programatica,
            if(
                sistema_origem <> "tpc", est.nome_limpo, "TPC"
            ) as estabelecimento_nome_limpo,
            if(
                sistema_origem <> "tpc", est.nome_sigla, "TPC"
            ) as estabelecimento_nome_sigla,
            if(
                sistema_origem <> "tpc", est.administracao, "direta"
            ) as estabelecimento_administracao,
            if(
                sistema_origem <> "tpc", est.responsavel_sms, "subpav"
            ) as estabelecimento_responsavel_sms, 
        from movimento as mov
        left join estabelecimento as est using (id_cnes)
    )

select *,
if (id_cnes in UNNEST( [
        "2970619",  -- CENTRO CARIOCA DO OLHO
        "2298120",  -- HOSPITAL MUNICIPAL ALBERT SCHWEITZER
        "2269481",  -- HOSPITAL MUNICIPAL PIEDADE
        "6716938", -- COORD DE EMERGENCIA REGIONAL CER BARRA
        "6716849", -- COORD DE EMERGENCIA REGIONAL CER LEBLON
        "2291266", -- HOSPITAL MUNICIPAL FRANCISCO DA SILVA TELLES
        "2269341", -- HOSPITAL MUNICIPAL JESUS
        "2270269" -- HOSPITAL MUNICIPAL MIGUEL COUTO
    ]), "nao", "sim") as dados_confiaveis,
from
    movimento_final

