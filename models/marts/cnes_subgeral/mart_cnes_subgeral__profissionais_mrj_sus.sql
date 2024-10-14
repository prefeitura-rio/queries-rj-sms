
{{
    config(
        enabled=true,
        schema="cnes_subgeral",
        alias="profissionais_mrj_sus"
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
    select * from {{ref("dim_profissional_sus_rio_historico")}}
),

profissional_dados_hci as (
    select distinct cns, dados, endereco from {{ ref("mart_historico_clinico__paciente") }}
),

final AS (
    SELECT
        STRUCT(
            p.data_particao,
            p.ano_competencia,
            p.mes_competencia,
            estabs.data_atualizao_registro,
            estabs.usuario_atualizador_registro,
            estabs.mes_particao,
            estabs.ano_particao,
            estabs.data_carga,
            estabs.data_snapshot
        ) AS metadados,

        STRUCT(
            p.id_cnes,
            estabs.id_unidade,
            estabs.nome_razao_social,
            estabs.nome_fantasia,
            estabs.nome_limpo,
            estabs.nome_sigla,
            estabs.nome_complemento,
            estabs.cnpj_mantenedora,
            estabs.esfera,
            estabs.id_natureza_juridica,
            estabs.natureza_juridica_descr,
            estabs.tipo_gestao,
            estabs.tipo_gestao_descr,
            estabs.responsavel_sms,
            estabs.administracao,
            estabs.diretor_clinico_cpf,
            estabs.diretor_clinico_conselho,
            estabs.tipo_turno,
            estabs.turno_atendimento,
            estabs.aberto_sempre,
            estabs.id_tipo_unidade,
            estabs.tipo,
            estabs.tipo_sms,
            estabs.tipo_sms_simplificado,
            estabs.tipo_sms_agrupado,
            estabs.tipo_unidade_alternativo,
            estabs.tipo_unidade_agrupado,
            estabs.id_ap,
            estabs.ap,
            estabs.endereco_cep,
            estabs.endereco_bairro,
            estabs.endereco_logradouro,
            estabs.endereco_numero,
            estabs.endereco_complemento,
            estabs.endereco_latitude,
            estabs.endereco_longitude,
            estabs.ativa,
            estabs.prontuario_tem,
            estabs.prontuario_versao,
            estabs.prontuario_estoque_tem_dado,
            estabs.prontuario_estoque_motivo_sem_dado,
            estabs.telefone,
            estabs.email,
            estabs.facebook,
            estabs.instagram,
            estabs.twitter,
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

        data_registro,
        cpf,
        p.cns,
        nome,
        vinculacao,
        vinculo_tipo,
        id_cbo,
        cbo,
        id_cbo_familia,
        cbo_familia,
        id_registro_conselho,
        id_tipo_conselho,
        carga_horaria_outros,
        carga_horaria_hospitalar,
        carga_horaria_ambulatorial,
        carga_horaria_total,
        dados as profissional_dados_hci,
        endereco as endereco_profissional_hci
            
    FROM profissionais_mrj AS p
    LEFT JOIN profissional_dados_hci AS hci ON SAFE_CAST(p.cns AS INT64) = (SELECT SAFE_CAST(cns AS INT64) FROM UNNEST(hci.cns) AS cns LIMIT 1)
    LEFT JOIN estabelecimentos_mrj_sus AS estabs ON p.ano_competencia = estabs.ano_competencia AND p.mes_competencia = estabs.mes_competencia AND SAFE_CAST(p.id_cnes AS INT64) = SAFE_CAST(estabs.id_cnes AS INT64)
)

select * from final