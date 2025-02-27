{{
    config(
        enabled=true,
        schema="projeto_cnes_subgeral",
        alias="equipamentos_mrj_sus",
        partition_by={
            "field": "metadado__data_particao",
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
            -- metadados
            equip.ano_competencia as metadado__ano_competencia,
            equip.mes_competencia as metadado__mes_competencia,
            data_atualizao_registro as metadado__data_atualizacao_registro,
            usuario_atualizador_registro as metadado__usuario_atualizador_registro,
            data_carga as metadado__data_carga,
            data_snapshot as metadado__data_snapshot,
            equip.data_particao as metadado__data_particao,

            -- estabelecimentos
            equip.id_cnes as estabelecimento__id_cnes,
            id_unidade as estabelecimento__id_unidade,
            nome_razao_social as estabelecimento__nome_razao_social,
            nome_fantasia as estabelecimento__nome_fantasia,
            nome_limpo as estabelecimento__nome_limpo,
            nome_sigla as estabelecimento__nome_sigla,
            nome_complemento as estabelecimento__nome_complemento,
            cnpj_mantenedora as estabelecimento__cnpj_mantenedora,
            esfera as estabelecimento__esfera,
            id_natureza_juridica as estabelecimento__id_natureza_juridica,
            natureza_juridica_descr as estabelecimento__natureza_juridica_descr,
            tipo_gestao as estabelecimento__tipo_gestao,
            tipo_gestao_descr as estabelecimento__tipo_gestao_descr,
            responsavel_sms as estabelecimento__responsavel_sms,
            administracao as estabelecimento__administracao,
            diretor_clinico_cpf as estabelecimento__diretor_clinico_cpf,
            diretor_clinico_conselho as estabelecimento__diretor_clinico_conselho,
            tipo_turno as estabelecimento__tipo_turno,
            turno_atendimento as estabelecimento__turno_atendimento,
            aberto_sempre as estabelecimento__aberto_sempre,
            id_tipo_unidade as estabelecimento__id_tipo_unidade,
            tipo as estabelecimento__tipo,
            tipo_sms as estabelecimento__tipo_sms,
            tipo_sms_simplificado as estabelecimento__tipo_sms_simplificado,
            tipo_sms_agrupado as estabelecimento__tipo_sms_agrupado,
            tipo_unidade_alternativo as estabelecimento__tipo_unidade_alternativo,
            tipo_unidade_agrupado as estabelecimento__tipo_unidade_agrupado,
            id_ap as estabelecimento__id_ap,
            ap as estabelecimento__ap,
            endereco_cep as estabelecimento__endereco_cep,
            endereco_bairro as estabelecimento__endereco_bairro,
            endereco_logradouro as estabelecimento__endereco_logradouro,
            endereco_numero as estabelecimento__endereco_numero,
            endereco_complemento as estabelecimento__endereco_complemento,
            endereco_latitude as estabelecimento__endereco_latitude,
            endereco_longitude as estabelecimento__endereco_longitude,
            ativa as estabelecimento__ativa,
            prontuario_tem as estabelecimento__prontuario_tem,
            prontuario_versao as estabelecimento__prontuario_versao,
            prontuario_estoque_tem_dado as estabelecimento__prontuario_estoque_tem_dado,
            prontuario_estoque_motivo_sem_dado
            as estabelecimento__prontuario_estoque_motivo_sem_dado,
            telefone as estabelecimento__telefone,
            email as estabelecimento__email,
            facebook as estabelecimento__facebook,
            instagram as estabelecimento__instagram,
            twitter as estabelecimento__twitter,
            estabs.estabelecimento_sms_indicador as estabelecimento__sms_indicador,
            vinculo_sus_indicador as estabelecimento__vinculo_sus_indicador,
            atendimento_internacao_sus_indicador
            as estabelecimento__atendimento_internacao_sus_indicador,
            atendimento_ambulatorial_sus_indicador
            as estabelecimento__atendimento_ambulatorial_sus_indicador,
            atendimento_sadt_sus_indicador
            as estabelecimento__atendimento_sadt_sus_indicador,
            atendimento_urgencia_sus_indicador
            as estabelecimento__atendimento_urgencia_sus_indicador,
            atendimento_outros_sus_indicador
            as estabelecimento__atendimento_outros_sus_indicador,
            atendimento_vigilancia_sus_indicador
            as estabelecimento__atendimento_vigilancia_sus_indicador,
            atendimento_regulacao_sus_indicador
            as estabelecimento__atendimento_regulacao_sus_indicador,

            -- equipamentos
            equipamento_tipo as equipamento__tipo,
            equipamento as equipamento__descr,
            equipamento_especifico_tipo as equipamento__tipo_especifico,
            equipamento_especifico as esquipamento__especifico_descr,
            equipamentos_quantidade as equipamento__quantidade,
            equipamentos_quantidade_ativos as equipamento__quantidade_ativos,

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
    metadado__data_particao = (select parse_date('%Y-%m-%d', versao) from versao_atual)
