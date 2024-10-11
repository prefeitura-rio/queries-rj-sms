{{
    config(
        enabled=true,
        schema="saude_cnes",
        alias="profissional_sus_rio_historico",
        partition_by = {
            'field': 'data_particao', 
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

with
versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
), 

estabelecimentos_mrj_sus as (
    select * from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

profissionais_mrj as (
    select * from {{ref("int_profissional_sus_rio_historico__brutos_filtrados")}}
),

cbo as (
    select * from {{ ref("raw_datasus__cbo") }}
),

cbo_fam as (
    select * from {{ ref("raw_datasus__cbo_fam") }}
),

tipo_vinculo as (
    select
        concat(id_vinculacao, tipo) as codigo_tipo_vinculo,
        descricao,
        data_particao
    from {{ ref("raw_cnes_web__tipo_vinculo") }}
    WHERE data_particao = (SELECT versao FROM versao_atual)
),

vinculo as (
    select
        id_vinculacao,
        descricao,
    from {{ ref("raw_cnes_web__vinculo") }}
    WHERE data_particao = (SELECT versao FROM versao_atual)
),

profissional_dados_hci as (
    select distinct cns, cpf from {{ ref("mart_historico_clinico__paciente") }}
),

final AS (
    SELECT
        parse_date('%Y-%m-%d', tipo_vinculo.data_particao) as data_particao,
        p.ano_competencia,
        p.mes_competencia,
        p.id_cnes,
        p.data_registro,
        hci.cpf,
        p.profissional_cns as cns,
        p.profissional_nome as nome,
        vinculacao.descricao AS vinculacao,
        tipo_vinculo.descricao AS vinculo_tipo,
        p.id_cbo,
        ocup.descricao AS cbo,
        p.id_cbo_familia,
        ocupf.descricao AS cbo_familia,
        id_registro_conselho,
        id_tipo_conselho,
        carga_horaria_outros,
        carga_horaria_hospitalar,
        carga_horaria_ambulatorial,
        carga_horaria_total,
            
    FROM profissionais_mrj AS p
    LEFT JOIN profissional_dados_hci AS hci ON SAFE_CAST(p.profissional_cns AS INT64) = (SELECT SAFE_CAST(cns AS INT64) FROM UNNEST(hci.cns) AS cns LIMIT 1)
    LEFT JOIN cbo AS ocup ON p.id_cbo = ocup.id_cbo
    LEFT JOIN cbo_fam AS ocupf ON LEFT(p.id_cbo_familia, 4) = ocupf.id_cbo_familia
    LEFT JOIN tipo_vinculo ON p.id_tipo_vinculo = tipo_vinculo.codigo_tipo_vinculo
    LEFT JOIN vinculo AS vinculacao ON p.id_vinculacao = vinculacao.id_vinculacao
)

select * from final