{{
    config(
        enabled=true,
        schema="projeto_cnes_subgeral",
        alias="leitos_mrj_sus",
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

leitos_mrj_sus_tabelao as (
    select 
        lt.ano_competencia,
        lt.mes_competencia,
        lt.id_cnes,
        lt.tipo_leito,
        lt.tipo_leito_descr,
        lt.tipo_especialidade_leito,
        lt.tipo_especialidade_leito_descr,
        lt.quantidade_total,
        lt.quantidade_contratado,
        lt.quantidade_sus,
        lt.data_particao,

        estabs.* except (id_cnes, ano_competencia, mes_competencia, data_particao)

    from  {{ref("dim_leito_sus_rio_historico")}} as lt
    left join estabelecimentos_mrj_sus as estabs on lt.ano_competencia = estabs.ano_competencia and lt.mes_competencia = estabs.mes_competencia and safe_cast(lt.id_cnes as int64) = safe_cast(estabs.id_cnes as int64)
),

final as (
    select
        STRUCT (
            data_particao,
            ano_competencia,
            mes_competencia,
            data_atualizao_registro,
            usuario_atualizador_registro,
            mes_particao,
            ano_particao,
            data_carga,
            data_snapshot
        ) as metadados,

        STRUCT(
            id_cnes,
            id_unidade,
            nome_razao_social,
            nome_fantasia,
            nome_limpo,
            nome_sigla,
            nome_complemento,
            cnpj_mantenedora,
            esfera,
            id_natureza_juridica,
            natureza_juridica_descr,
            tipo_gestao,
            tipo_gestao_descr,
            responsavel_sms,
            administracao,
            diretor_clinico_cpf,
            diretor_clinico_conselho,
            tipo_turno,
            turno_atendimento,
            aberto_sempre,
            id_tipo_unidade,
            tipo,
            tipo_sms,
            tipo_sms_simplificado,
            tipo_sms_agrupado,
            tipo_unidade_alternativo,
            tipo_unidade_agrupado,
            id_ap,
            ap,
            endereco_cep,
            endereco_bairro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,
            endereco_latitude,
            endereco_longitude,
            ativa,
            prontuario_tem,
            prontuario_versao,
            prontuario_estoque_tem_dado,
            prontuario_estoque_motivo_sem_dado,
            telefone,
            email,
            facebook,
            instagram,
            twitter,
            estabelecimento_sms_indicador,
            vinculo_sus_indicador,
            atendimento_internacao_sus_indicador,	
            atendimento_ambulatorial_sus_indicador,
            atendimento_sadt_sus_indicador,
            atendimento_urgencia_sus_indicador,  
            atendimento_outros_sus_indicador, 
            atendimento_vigilancia_sus_indicador,
            atendimento_regulacao_sus_indicador
        ) as estabelecimentos,

        tipo_leito,
        tipo_leito_descr,
        tipo_especialidade_leito,
        tipo_especialidade_leito_descr,
        quantidade_total,
        quantidade_contratado,
        quantidade_sus

    from leitos_mrj_sus_tabelao
)

select * from final where metadados.data_particao = (select parse_date('%Y-%m-%d', versao) from versao_atual)