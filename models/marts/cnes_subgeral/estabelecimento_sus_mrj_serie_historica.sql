{{
    config(
        schema="mart_cnes_subgeral__estabelecimento_sus_mrj_serie_historica",
        alias="estabelecimento_sus_mrj_serie_historica"
    )
}}

WITH estabelecimentos_brutos AS (
    SELECT * 
    FROM {{ ref("raw_cnes_subgeral__estabelecimento_sus_mrj_serie_historica") }}
),

estabelecimentos_atributos_cnes_web AS (
    SELECT
        DISTINCT id_cnes,
        id_tipo_estabelecimento, 
        nome_razao_social,
        nome_fantasia,
        endereco_logradouro,
        endereco_numero,
        endereco_complemento,
        endereco_bairro,
        endereco_latitude,
        endereco_longitude
    FROM {{ ref("raw_cnes_web__estabelecimento") }}
),

estabelecimentos_atributos_subgeral AS (
    SELECT 
        ESFERA as esfera,
        AP as id_ap,
        AP_TITULO as ap,
        CNES as id_estabelecimento_cnes,
        TIPO_UNIDADE_AGRUPADO as tipo_unidade_agrupado,

    FROM {{ source("subgeral_padronizacoes", "padronizacao_estabelecimentos") }}
),

tp_gestao AS (
    SELECT 
        DISTINCT id_tipo_gestao,
        UPPER(tipo_gestao_descr) as tipo_gestao_descr
    FROM {{ source("brutos_datasus", "tipo_gestao") }}
),

nat_jur AS (
    SELECT 
        DISTINCT CO_NATUREZA_JUR AS id_nat_jur, 
        DS_NATUREZA_JUR AS natureza_juridica 
    FROM {{ source("brutos_cnes_web_staging", "tbNaturezaJuridica") }}
),

tp_unidade AS (
    SELECT 
        DISTINCT id_tipo_unidade,
        descricao AS tipo_unidade_descr 
    FROM {{ ref("raw_cnes_web__tipo_unidade") }}
),

turno AS (
    SELECT 
        DISTINCT id_turno_atendimento, 
        descricao AS turno_atendimento 
    FROM {{ ref("raw_cnes_web__turno_atendimento") }}
),

joined AS (
    SELECT
        brutos.*,

        cnes_web.nome_razao_social,
        cnes_web.nome_fantasia,
        cnes_web.endereco_logradouro,
        cnes_web.endereco_numero,
        cnes_web.endereco_complemento,
        cnes_web.endereco_bairro,
        cnes_web.endereco_latitude,
        cnes_web.endereco_longitude,

        subgeral.tipo_unidade_agrupado,
        subgeral.esfera,
        subgeral.id_ap,
        subgeral.ap,

        tp_gestao.tipo_gestao_descr,
        nat_jur.natureza_juridica,
        tp_unidade.tipo_unidade_descr,
        turno.turno_atendimento,

        CASE 
            when (cnes_web.nome_fantasia like 'SMS %') or (cnes_web.nome_fantasia = 'RIOSAUDE') or (cnes_web.nome_fantasia = 'SMSDC %') then 1
            else 0 
        END AS indicador_estabelecimento_sms

    FROM estabelecimentos_brutos AS brutos

    LEFT JOIN estabelecimentos_atributos_cnes_web AS cnes_web 
    ON cast(brutos.id_estabelecimento_cnes as int) = cast(cnes_web.id_cnes as int)

    LEFT JOIN estabelecimentos_atributos_subgeral AS subgeral 
    ON cast(brutos.id_estabelecimento_cnes as int) = cast(subgeral.id_estabelecimento_cnes as int)

    LEFT JOIN nat_jur
    ON cast(brutos.id_natureza_juridica as int) = cast(nat_jur.id_nat_jur as int)

    LEFT JOIN tp_unidade
    ON cast(brutos.tipo_unidade as int) = cast(tp_unidade.id_tipo_unidade as int)

    LEFT JOIN turno
    ON cast(brutos.tipo_turno as int) = cast(turno.id_turno_atendimento as int)

    LEFT JOIN tp_gestao
    ON brutos.tipo_gestao = tp_gestao.id_tipo_gestao
)

SELECT 
    ano,
    mes,
    esfera,
    id_ap,
    ap,
    cep,
    id_estabelecimento_cnes,
    nome_razao_social,
    nome_fantasia,
    id_natureza_juridica,
    natureza_juridica,
    tipo_gestao,
    tipo_gestao_descr,
    tipo_unidade,
    tipo_unidade_descr,
    tipo_unidade_agrupado,
    tipo_turno,
    turno_atendimento,
    endereco_bairro,
    endereco_logradouro,
    endereco_numero,
    endereco_complemento,
    endereco_latitude,
    endereco_longitude,
    indicador_estabelecimento_sms,
    indicador_vinculo_sus,
    indicador_atendimento_internacao_sus,	
    indicador_atendimento_ambulatorial_sus,
    indicador_atendimento_sadt_sus,
    indicador_atendimento_urgencia_sus,  
    indicador_atendimento_outros_sus, 
    indicador_atendimento_vigilancia_sus,
    indicador_atendimento_regulacao_sus

FROM joined