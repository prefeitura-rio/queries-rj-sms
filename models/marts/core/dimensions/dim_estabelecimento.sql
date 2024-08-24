{{ config(materialized="table", alias="estabelecimento", tags=["daily"]) }}


with
    versao_atual as (
        select max(data_particao) as versao from {{ ref("raw_cnes_web__tipo_unidade") }}
    ),

    estabelecimento as (
        select *
        from {{ ref("raw_cnes_web__estabelecimento") }}
        where data_particao = (select versao from versao_atual)
    ),

    unidade as (
        select *
        from {{ ref("raw_cnes_web__tipo_unidade") }}
        where data_particao = (select versao from versao_atual)
    ),

    turno as (
        select *
        from {{ ref("raw_cnes_web__turno_atendimento") }}
        where data_particao = (select versao from versao_atual)
    ),

    estab_sms as (
        select *
        from estabelecimento
        where
            cnpj_mantenedora = "29468055000102"  -- SMS-RIO
            or id_cnes = "5456932"  -- Fio Cruz
            or (id_municipio_gestor = "330455" and id_natureza_juridica = "2011")  -- Rio de Janeiro & Empresa Publica (Rio Saúde)
            or (id_municipio_gestor = "330455" and id_natureza_juridica = "1031")  -- Rio de Janeiro & Orgao Publico do Poder Executivo Municipal
    ),

    estab_aux as (select * from {{ ref("raw_sheets__estabelecimento_auxiliar") }}),

    contatos_aps as (
        select * from {{ ref("raw_plataforma_smsrio__estabelecimento_contato") }}
    ),

    estab_final as (
        select
            estab_sms.*,
            split(estab_sms.telefone, "/") as telefone_cnes,
            contatos_aps.telefone as telefone_aps,
            estab_sms.email as email_cnes,
            contatos_aps.email as email_aps,
            contatos_aps.facebook,
            contatos_aps.instagram,
            contatos_aps.twitter,
            estab_aux.agrupador_sms,
            estab_aux.tipo_sms,
            estab_aux.tipo_sms_simplificado,
            estab_aux.nome_limpo,
            regexp_replace(
                estab_aux.nome_limpo,
                r'(CF |CSE |CMS |UPA 24h |POLICLINICA |HOSPITAL MUNICIPAL |COORD DE EMERGENCIA REGIONAL CER |MATERNIDADE )',
                ''
            ) as nome_complemento,
            estab_aux.nome_sigla,
            estab_aux.prontuario_tem,
            estab_aux.prontuario_versao,
            estab_aux.responsavel_sms,
            estab_aux.administracao,
            estab_aux.prontuario_estoque_tem_dado,
            estab_aux.prontuario_estoque_motivo_sem_dado,
            coalesce(
                estab_aux.area_programatica, estab_sms.id_distrito_sanitario
            ) as id_distrito_sanitario_corrigido,  -- corrige registros que possuem algum erro no cadsus
        from estab_sms
        left join estab_aux using (id_cnes)
        left join contatos_aps using (id_cnes)
    )

select
    -- Primary key
    est.id_unidade,

    -- Foreign keys
    est.id_cnes,
    est.id_tipo_unidade,
    est.id_distrito_sanitario_corrigido as area_programatica,
    est.cnpj_mantenedora,

    -- Common fields
    if(est.id_motivo_desativacao = "", "sim", "não") as ativa,
    est.agrupador_sms as tipo_sms_agrupado,
    unidade.descricao as tipo,  # TODO: renomear para tipo_cnes
    est.tipo_sms,
    est.tipo_sms_simplificado,
    est.nome_limpo,
    est.nome_sigla,
    est.nome_complemento,
    est.nome_fantasia,
    est.responsavel_sms,
    est.administracao,
    est.prontuario_tem,
    est.prontuario_versao,
    est.prontuario_estoque_tem_dado,
    est.prontuario_estoque_motivo_sem_dado,
    est.endereco_bairro,
    est.endereco_logradouro,
    est.endereco_numero,
    est.endereco_complemento,
    est.endereco_cep,
    est.endereco_latitude,
    est.endereco_longitude,
    coalesce(est.telefone_aps, est.telefone_cnes) as telefone,
    coalesce(est.email_aps, est.email_cnes) as email,
    est.facebook,
    est.instagram,
    est.twitter,
    est.aberto_sempre,
    turno.descricao as turno_atendimento,
    est.diretor_clinico_cpf,
    est.diretor_clinico_conselho,

    -- Metadata
    data_atualizao_registro,
    usuario_atualizador_registro,
    est.mes_particao,
    est.ano_particao,
    est.data_particao,
    est.data_carga,
    est.data_snapshot,

from estab_final as est
left join turno using (id_turno_atendimento)
left join unidade using (id_tipo_unidade)

order by
    ativa desc,
    est.id_tipo_unidade asc,
    area_programatica asc,
    est.endereco_bairro asc,
    est.nome_fantasia asc
