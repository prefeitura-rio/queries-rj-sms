{{
    config(
        enabled=true,
        schema="projeto_cnes_subgeral",
        alias="profissionais_mrj_sus",
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

    profissionais_mrj as (
        select * from {{ ref("dim_profissional_sus_rio_historico") }}
    ),

    profissional_dados_hci as (
        select distinct cns, dados, endereco
        from {{ ref("mart_historico_clinico__paciente") }}
    ),

    final as (
        select
            -- metadado
            p.data_particao as metadado__data_particao,
            p.ano_competencia as metadado__ano_competencia,
            p.mes_competencia as metadado__mes_competencia,
            data_registro as metadado__data_registro,
            estabs.data_atualizao_registro as metadado__data_atualizacao_registro,
            estabs.usuario_atualizador_registro
            as metadado__usuario_atualizador_registro,
            estabs.data_carga as metadado__data_carga,
            estabs.data_snapshot as metadado__data_snapshot,

            -- estabelecimento
            p.id_cnes as estabelecimento__id_cnes,
            estabs.id_unidade as estabelecimento__id_unidade,
            estabs.nome_razao_social as estabelecimento__nome_razao_social,
            estabs.nome_fantasia as estabelecimento__nome_fantasia,
            estabs.nome_limpo as estabelecimento__nome_limpo,
            estabs.nome_sigla as estabelecimento__nome_sigla,
            estabs.nome_complemento as estabelecimento__nome_complemento,
            estabs.cnpj_mantenedora as estabelecimento__cnpj_mantenedora,
            estabs.esfera as estabelecimento__esfera,
            estabs.id_natureza_juridica as estabelecimento__id_natureza_juridica,
            estabs.natureza_juridica_descr as estabelecimento__natureza_juridica_descr,
            estabs.tipo_gestao as estabelecimento__tipo_gestao,
            estabs.tipo_gestao_descr as estabelecimento__tipo_gestao_descr,
            estabs.responsavel_sms as estabelecimento__responsavel_sms,
            estabs.administracao as estabelecimento__administracao,
            estabs.diretor_clinico_cpf as estabelecimento__diretor_clinico_cpf,
            estabs.diretor_clinico_conselho
            as estabelecimento__diretor_clinico_conselho,
            estabs.tipo_turno as estabelecimento__tipo_turno,
            estabs.turno_atendimento as estabelecimento__turno_atendimento,
            estabs.aberto_sempre as estabelecimento__aberto_sempre,
            estabs.id_tipo_unidade as estabelecimento__id_tipo_unidade,
            estabs.tipo as estabelecimento__tipo,
            estabs.tipo_sms as estabelecimento__tipo_sms,
            estabs.tipo_sms_simplificado as estabelecimento__tipo_sms_simplificado,
            estabs.tipo_sms_agrupado as estabelecimento__tipo_sms_agrupado,
            estabs.tipo_unidade_alternativo
            as estabelecimento__tipo_unidade_alternativo,
            estabs.tipo_unidade_agrupado as estabelecimento__tipo_unidade_agrupado,
            estabs.id_ap as estabelecimento__id_ap,
            estabs.ap as estabelecimento__ap,
            estabs.endereco_cep as estabelecimento__endereco_cep,
            estabs.endereco_bairro as estabelecimento__endereco_bairro,
            estabs.endereco_logradouro as estabelecimento__endereco_logradouro,
            estabs.endereco_numero as estabelecimento__endereco_numero,
            estabs.endereco_complemento as estabelecimento__endereco_complemento,
            estabs.endereco_latitude as estabelecimento__endereco_latitude,
            estabs.endereco_longitude as estabelecimento__endereco_longitude,
            estabs.ativa as estabelecimento__ativa,
            estabs.prontuario_tem as estabelecimento__prontuario_tem,
            estabs.prontuario_versao as estabelecimento__prontuario_versao,
            estabs.prontuario_estoque_tem_dado
            as estabelecimento__prontuario_estoque_tem_dado,
            estabs.prontuario_estoque_motivo_sem_dado
            as estabelecimento__prontuario_estoque_motivo_sem_dado,
            estabs.telefone as estabelecimento__telefone,
            estabs.email as estabelecimento__email,
            estabs.facebook as estabelecimento__facebook,
            estabs.instagram as estabelecimento__instagram,
            estabs.twitter as estabelecimento__twitter,
            estabs.estabelecimento_sms_indicador as estabelecimento__sms_indicador,
            estabs.vinculo_sus_indicador as estabelecimento__vinculo_sus_indicador,
            estabs.atendimento_internacao_sus_indicador
            as estabelecimento__atendimento_internacao_sus_indicador,
            estabs.atendimento_ambulatorial_sus_indicador
            as estabelecimento__atendimento_ambulatorial_sus_indicador,
            estabs.atendimento_sadt_sus_indicador
            as estabelecimento__atendimento_sadt_sus_indicador,
            estabs.atendimento_urgencia_sus_indicador
            as estabelecimento__atendimento_urgencia_sus_indicador,
            estabs.atendimento_outros_sus_indicador
            as estabelecimento__atendimento_outros_sus_indicador,
            estabs.atendimento_vigilancia_sus_indicador
            as estabelecimento__atendimento_vigilancia_sus_indicador,
            estabs.atendimento_regulacao_sus_indicador
            as estabelecimento__atendimento_regulacao_sus_indicador,

            -- profissional
            cpf as profissional__cpf,
            p.cns as profissional__cns,
            nome as profissional__nome,
            dados.data_nascimento as profissional__data_nascimento,
            date_diff(
                date(p.ano_competencia, p.mes_competencia, 1),
                dados.data_nascimento,
                year
            ) as profissional__idade,
            dados.mae_nome as profissional__mae_nome,
            dados.pai_nome as profissional__pai_nome,
            dados.genero as profissional__genero,
            dados.raca as profissional__raca,
            endereco as profissional__endereco,
            vinculacao as profissional__vinculacao,
            vinculo_tipo as profissional__vinculacao_tipo,
            id_cbo as profissional__id_cbo,
            upper(cbo) as profissional__cbo,
            id_cbo_familia as profissional__id_cbo_familia,
            upper(cbo_familia) as profissional__cbo_familia,
            id_registro_conselho as profissional__id_registro_conselho,
            id_tipo_conselho as profissional__id_tipo_conselho,
            carga_horaria_hospitalar as profissional__carga_horaria_hospitalar,
            carga_horaria_ambulatorial as profissional__carga_horaria_ambulatorial,
            carga_horaria_outros as profissional__carga_horaria_outros,
            carga_horaria_total as profissional__carga_horaria_total,
            dados.obito_indicador as profissional__obito_indicador,
            dados.obito_data as profissional__obito_data,

        from profissionais_mrj as p
        left join
            profissional_dados_hci as hci
            on safe_cast(p.cns as int64)
            = (select safe_cast(cns as int64) from unnest(hci.cns) as cns limit 1)
        left join
            estabelecimentos_mrj_sus as estabs
            on p.ano_competencia = estabs.ano_competencia
            and p.mes_competencia = estabs.mes_competencia
            and safe_cast(p.id_cnes as int64) = safe_cast(estabs.id_cnes as int64)
    )

select *
from final
