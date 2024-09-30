{{
    config(
        schema="saude_cnes",
        alias="profissional_sus_rio_historico",
        materialized="table",
        tags=["weekly"],
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

profissionais_mrj_non_unique as (
select
    ano,
    mes,
    concat(ano, '-', lpad(cast(mes as string), 2, '0')) data_registro,
    id_estabelecimento_cnes,
    sigla_uf,
    case 
        when cartao_nacional_saude = "nan" then NULL 
        else cartao_nacional_saude 
    end as profissional_cns,
    nome as profissional_nome,
    cbo_2002 as id_cbo,
    substring(tipo_vinculo, 1, 4) as id_tipo_vinculo,
    substring(tipo_vinculo, 1, 2) as id_vinculacao,
    left(cbo_2002, 4) as id_cbo_familia,
    id_registro_conselho,
    tipo_conselho as id_tipo_conselho,
    carga_horaria_outros,
    carga_horaria_hospitalar,
    carga_horaria_ambulatorial,
    carga_horaria_outros + carga_horaria_hospitalar + carga_horaria_ambulatorial as carga_horaria_total

from {{ ref("raw_cnes_ftp__profissional") }}

where
    ano >= 2008
    and sigla_uf = "RJ"
    and (
        indicador_atende_sus = 1
        or indicador_vinculo_contratado_sus = 1
        or indicador_vinculo_autonomo_sus = 1
    )
    and safe_cast(id_estabelecimento_cnes as int64) in (select distinct safe_cast(id_cnes as int64) from estabelecimentos_mrj_sus)
),

profissionais_mrj as (
    select distinct * from profissionais_mrj_non_unique
),

/* --- GERANDO CARDINALIDADE:
profissionais_cnesweb as (
    select
        distinct cns,
        id_codigo_sus,
        nome,
        data_atualizacao,
        row_number() over (partition by nome, id_codigo_sus, cns order by data_atualizacao desc) as ordenacao
    from {{ ref("raw_cnes_web__dados_profissional_sus") }}
    WHERE data_particao = (SELECT versao FROM versao_atual) and cns != ""
),
*/

cbo as (select * from {{ ref("raw_datasus__cbo") }}),
cbo_fam as (select * from {{ ref("raw_datasus__cbo_fam") }}),

tipo_vinculo as (
    select
        concat(id_vinculacao, tipo) as codigo_tipo_vinculo,
        descricao,
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

profissional_dados_hci as (select distinct cns, cpf, dados, endereco from {{ ref("mart_historico_clinico__paciente") }}),

final AS (
    SELECT
        STRUCT(
            estabs.data_atualizao_registro,
            estabs.usuario_atualizador_registro,
            estabs.mes_particao,
            estabs.ano_particao,
            estabs.data_particao,
            estabs.data_carga,
            estabs.data_snapshot
        ) AS metadados,

        STRUCT(
            -- Identificação
            estabs.ano,
            estabs.mes,
            estabs.id_cnes,
            estabs.id_unidade,
            estabs.nome_razao_social,
            estabs.nome_fantasia,
            estabs.nome_limpo,
            estabs.nome_sigla,
            estabs.nome_complemento,
            estabs.cnpj_mantenedora,

            -- Responsabilização
            estabs.esfera,
            estabs.id_natureza_juridica,
            estabs.natureza_juridica_descr,
            estabs.tipo_gestao,
            estabs.tipo_gestao_descr,
            estabs.responsavel_sms,
            estabs.administracao,
            estabs.diretor_clinico_cpf,
            estabs.diretor_clinico_conselho,

            -- Atributos
            estabs.tipo_turno,
            estabs.turno_atendimento,
            estabs.aberto_sempre,

            -- Tipagem dos Estabelecimentos (CNES)
            estabs.id_tipo_unidade,
            estabs.tipo,

            -- Tipagem dos Estabelecimentos (DIT)
            estabs.tipo_sms,
            estabs.tipo_sms_simplificado,
            estabs.tipo_sms_agrupado,

            -- Tipagem dos Estabelecimentos (SUBGERAL)
            estabs.tipo_unidade_alternativo,
            estabs.tipo_unidade_agrupado,

            -- Localização
            estabs.id_ap,
            estabs.ap,
            estabs.endereco_cep,
            estabs.endereco_bairro,
            estabs.endereco_logradouro,
            estabs.endereco_numero,
            estabs.endereco_complemento,
            estabs.endereco_latitude,
            estabs.endereco_longitude,

            -- Status
            estabs.ativa,

            -- Prontuário
            estabs.prontuario_tem,
            estabs.prontuario_versao,
            estabs.prontuario_estoque_tem_dado,
            estabs.prontuario_estoque_motivo_sem_dado,

            -- Informações de contato
            estabs.telefone,
            estabs.email,
            estabs.facebook,
            estabs.instagram,
            estabs.twitter,

            -- Indicadores
            estabs.estabelecimento_sms_indicador,
            estabs.vinculo_sus_indicador,
            estabs.atendimento_internacao_sus_indicador,	
            estabs.atendimento_ambulatorial_sus_indicador,
            estabs.atendimento_sadt_sus_indicador,
            estabs.atendimento_urgencia_sus_indicador,  
            estabs.atendimento_outros_sus_indicador, 
            estabs.atendimento_vigilancia_sus_indicador,
            estabs.atendimento_regulacao_sus_indicador
        ) AS estabelecimentos,

        STRUCT(
            --cod_sus.id_codigo_sus as profissional_codigo_sus,
            p.data_registro,
            hci.cpf,
            p.profissional_cns,
            p.profissional_nome,
            vinculacao.descricao AS vinculacao,
            tipo_vinculo.descricao AS vinculo_tipo,
            p.id_cbo,
            ocup.descricao AS cbo,
            p.id_cbo_familia,
            ocupf.descricao AS cbo_familia,
            p.id_registro_conselho,
            p.id_tipo_conselho,
            hci.dados as profissional_dados_hci,
            hci.endereco as endereco_profissional_hci,
            p.carga_horaria_outros,
            p.carga_horaria_hospitalar,
            p.carga_horaria_ambulatorial,
            p.carga_horaria_total
        ) AS profissionais
        
    FROM profissionais_mrj AS p
    LEFT JOIN profissional_dados_hci AS hci ON SAFE_CAST(p.profissional_cns AS INT64) = (SELECT SAFE_CAST(cns AS INT64) FROM UNNEST(hci.cns) AS cns LIMIT 1)
    LEFT JOIN estabelecimentos_mrj_sus AS estabs ON p.ano = estabs.ano AND p.mes = estabs.mes AND SAFE_CAST(p.id_estabelecimento_cnes AS INT64) = SAFE_CAST(estabs.id_cnes AS INT64)
    LEFT JOIN cbo AS ocup ON p.id_cbo = ocup.id_cbo
    LEFT JOIN cbo_fam AS ocupf ON LEFT(p.id_cbo_familia, 4) = ocupf.id_cbo_familia
    LEFT JOIN tipo_vinculo ON p.id_tipo_vinculo = tipo_vinculo.codigo_tipo_vinculo
    LEFT JOIN vinculo AS vinculacao ON p.id_vinculacao = vinculacao.id_vinculacao
    -- Removido temporariamente por estar gerando cardinalidade (cada cns unico possui mais de um cod_sus associado no cnes web)
    --left join (select * from profissionais_cnesweb where ordenacao = 1) as cod_sus on p.profissional_cns = cod_sus.cns
)

select * from final