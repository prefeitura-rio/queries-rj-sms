{{
    config(
        schema="saude_cnes",
        alias="equipamento_sus_rio_historico"
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

equip as (
  select 
    ano,
    mes,
    lpad(id_estabelecimento_cnes, 7, '0') as id_cnes,
    safe_cast(tipo_equipamento as int64) as equipamento_tipo,
    safe_cast(id_equipamento as int64) as equipamento_especifico_tipo,
    safe_cast(quantidade_equipamentos as int64) as equipamentos_quantidade,
    safe_cast(quantidade_equipamentos_ativos as int64) as equipamentos_quantidade_ativos,

  from {{ ref("raw_cnes_ftp__equipamento") }}
  where indicador_equipamento_disponivel_sus = 1 and ano >= 2008 and safe_cast(id_estabelecimento_cnes as int64) in (select distinct safe_cast(id_cnes as int64) from estabelecimentos_mrj_sus)
),

equip_mapping_geral as (
  select
    equipamento_tipo,
    equipamento
  from {{ref ("raw_cnes_web__tipo_equipamento") }}
  where data_particao = (SELECT versao FROM versao_atual)
),

equip_mapping_especifico as (
  select
    equipamento_especifico_tipo,
    equipamento_tipo,
    equipamento_especifico
  from {{ref ("raw_cnes_web__tipo_equipamento_especifico") }}
  where data_particao = (SELECT versao FROM versao_atual)
),

final as (
    select
        struct (
            data_atualizao_registro,
            usuario_atualizador_registro,
            mes_particao,
            ano_particao,
            data_particao,
            data_carga,
            data_snapshot
        ) as metadados,

        struct (
            -- Identificação
            estabs.ano,
            estabs.mes,
            estabs.id_cnes,
            id_unidade,
            nome_razao_social,
            nome_fantasia,
            nome_limpo,
            nome_sigla,
            nome_complemento,
            cnpj_mantenedora,

            -- Responsabilização
            esfera,
            id_natureza_juridica,
            natureza_juridica_descr,
            tipo_gestao,
            tipo_gestao_descr,
            responsavel_sms,
            administracao,
            diretor_clinico_cpf,
            diretor_clinico_conselho,

            -- Atributos
            tipo_turno,
            turno_atendimento,
            aberto_sempre,

            -- Tipagem dos Estabelecimentos (CNES)
            id_tipo_unidade,
            tipo,

            -- Tipagem dos Estabelecimentos (DIT)
            tipo_sms,
            tipo_sms_simplificado,
            tipo_sms_agrupado,

            -- Tipagem dos Estabelecimentos (SUBGERAL)
            tipo_unidade_alternativo,
            tipo_unidade_agrupado,

            -- Localização
            id_ap,
            ap,
            endereco_cep,
            endereco_bairro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,
            endereco_latitude,
            endereco_longitude,

            -- Status
            ativa,

            -- Prontuário
            prontuario_tem,
            prontuario_versao,
            prontuario_estoque_tem_dado,
            prontuario_estoque_motivo_sem_dado,

            -- Informações de contato
            telefone,
            email,
            facebook,
            instagram,
            twitter,

            -- Indicadores
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

        struct(
            equip.equipamento_tipo,
            equipamento,
            equipamento_especifico_tipo,
            equipamento_especifico,
            equipamentos_quantidade,
            equipamentos_quantidade_ativos
        ) as equipamentos

    from equip
    left join estabelecimentos_mrj_sus as estabs using (id_cnes, ano, mes)
    left join equip_mapping_geral as map_geral using (equipamento_tipo)
    left join equip_mapping_especifico as map_espec using (equipamento_tipo, equipamento_especifico_tipo)
)

select * from final