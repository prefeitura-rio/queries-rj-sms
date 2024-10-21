{{
    config(
        enabled=true,
        schema="projeto_cnes_subgeral",
        alias="equipamentos_mrj_sus",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    versao_atual as (
        select max(data_particao) as versao from {{ ref("raw_cnes_web__tipo_unidade") }}
    ),

    estabelecimentos_mrj_sus as (
        select *
        from {{ ref("dim_estabelecimento_sus_rio_historico") }}
        where safe_cast(data_particao as string) = (select versao from versao_atual)
    ),

    equip as (select * from {{ ref("dim_equipamento_sus_rio_historico") }}),

    final as (
        select
            struct(
                equip.ano_competencia,
                equip.mes_competencia,
                data_atualizao_registro,
                usuario_atualizador_registro,
                data_carga,
                data_snapshot
            ) as metadados,

            struct(
                equip.id_cnes,
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
                estabs.estabelecimento_sms_indicador,
                vinculo_sus_indicador,
                atendimento_internacao_sus_indicador,
                atendimento_ambulatorial_sus_indicador,
                atendimento_sadt_sus_indicador,
                atendimento_urgencia_sus_indicador,
                atendimento_outros_sus_indicador,
                atendimento_vigilancia_sus_indicador,
                atendimento_regulacao_sus_indicador
            ) as estabelecimentos,

            equipamento_tipo,
            equipamento,
            equipamento_especifico_tipo,
            equipamento_especifico,
            equipamentos_quantidade,
            equipamentos_quantidade_ativos,

            equip.data_particao,

        from equip
        left join
            estabelecimentos_mrj_sus as estabs
            on equip.id_cnes = estabs.id_cnes
            and equip.ano_competencia = estabs.ano_competencia
            and equip.mes_competencia = estabs.mes_competencia
    )

select *
from final
where
    data_particao = (select parse_date('%Y-%m-%d', versao) from versao_atual)
