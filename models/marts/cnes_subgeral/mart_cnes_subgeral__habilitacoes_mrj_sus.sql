{{
    config(
        enabled=true,
        schema="projeto_cnes_subgeral",
        alias="habilitacoes_mrj_sus",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    versao_atual as (
        select max(data_particao) as versao
        from {{ ref("dim_habilitacao_sus_rio_historico") }}
    ),

    estabelecimentos_mrj_sus as (
        select *
        from {{ ref("dim_estabelecimento_sus_rio_historico") }}
        where
            safe_cast(data_particao as string)
            = (select safe_cast(versao as string) as versao from versao_atual)
    ),

    habilitacoes as (select * from {{ ref("dim_habilitacao_sus_rio_historico") }}),

    final as (
        select
            struct(
                hab.ano_competencia,
                hab.mes_competencia,
                data_atualizao_registro,
                usuario_atualizador_registro,
                data_carga,
                data_snapshot
            ) as metadados,

            struct(
                hab.id_cnes,
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

            id_habilitacao,
            habilitacao,
            habilitacao_ativa_indicador,
            nivel_habilitacao,
            tipo_origem,
            habilitacao_ano_inicio,
            habilitacao_mes_inicio,
            habilitacao_ano_fim,
            habilitacao_mes_fim,

            hab.data_particao,

        from habilitacoes as hab
        left join
            estabelecimentos_mrj_sus as estabs
            on hab.ano_competencia = estabs.ano_competencia
            and hab.mes_competencia = estabs.mes_competencia
            and safe_cast(hab.id_cnes as int64) = safe_cast(estabs.id_cnes as int64)
    )

select *
from final
