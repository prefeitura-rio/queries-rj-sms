{{
    config(
        enabled=true,
        schema="projeto_cnes_subgeral",
        alias="leitos_mrj_sus",
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

        from {{ ref("dim_leito_sus_rio_historico") }} as lt
        left join
            estabelecimentos_mrj_sus as estabs
            on lt.ano_competencia = estabs.ano_competencia
            and lt.mes_competencia = estabs.mes_competencia
            and safe_cast(lt.id_cnes as int64) = safe_cast(estabs.id_cnes as int64)
    ),

    final as (
        select
            -- metadados
            ano_competencia as metadado__ano_competencia,
            mes_competencia as metadado__mes_competencia,
            data_atualizao_registro as metadado__data_atualizacao_registro,
            usuario_atualizador_registro as metadado__usuario_atualizador_registro,
            data_carga as metadado__data_carga,
            data_snapshot as metadado__data_snapshot,
            data_particao as metadado__data_particao,

            -- estabelecimentos
            id_cnes as estabelecimento__id_cnes,
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
            estabelecimento_sms_indicador as estabelecimento__sms_indicador,
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

            -- leitos
            tipo_leito as leito__tipo,
            tipo_leito_descr as leito__tipo_descr,
            tipo_especialidade_leito as leito__tipo_especialidade,
            tipo_especialidade_leito_descr as leito__tipo_especialidade_descr,
            quantidade_contratado as leito__quantidade_contratado,
            quantidade_sus as leito__quantidade_sus,
            quantidade_total as leito__quantidade_total

        from leitos_mrj_sus_tabelao
    )

select *
from final
where
    metadado__data_particao = (select parse_date('%Y-%m-%d', versao) from versao_atual)
