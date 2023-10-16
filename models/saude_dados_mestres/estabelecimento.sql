{{
    config(
        materialized="table",
    )
}}


with
    estab_sms as (
        select *
        from {{ ref("brutos_cnes__estabelecimento") }}
        where
            cnpj_mantenedora = "29468055000102"  -- SMS-RIO
            or id_cnes = "5456932"  -- Fio Cruz
            or (id_municipio_gestor = "330455" and id_natureza_juridica = "2011")  -- Rio de Janeiro & Empresa Publica
            or (id_municipio_gestor = "330455" and id_natureza_juridica = "1031")  -- Rio de Janeiro & Orgao Publico do Poder Executivo Municipal
    ),
    estab_aux as (select * from {{ ref("estabelecimento_auxiliar") }}),
    estab_final as (
        select
            estab_sms.*,
            estab_aux.nome_limpo,
            estab_aux.nome_sigla,
            estab_aux.prontuario,
            estab_aux.administracao,
            coalesce(
                estab_aux.area_programatica, estab_sms.id_distrito_sanitario
            ) as id_distrito_sanitario_corrigido,  -- corrige registros que possuem algum erro no cadsus
        from estab_sms
        left join estab_aux using (id_cnes)
    ),
    atencao as (
        select gestao.id_unidade, tipo.*
        from {{ ref("gestao_nivel_atencao") }} as gestao
        left join {{ ref("tipo_nivel_atencao") }} as tipo using (id_nivel_atencao)
    ),
    unidade as (select * from {{ ref("tipo_unidade") }}),
    turno as (select * from {{ ref("turno_atendimento") }})

select
    -- Primary key
    est.id_unidade,

    -- Foreign keys
    est.id_cnes,
    est.id_tipo_unidade,
    est.id_distrito_sanitario_corrigido as area_programatica,

    -- Common fields
    IF (est.id_motivo_desativacao is null, "sim", "não") as ativa,
    unidade.descricao as tipo,
    est.nome_limpo,
    est.nome_sigla,
    est.nome_fantasia,
    est.administracao,
    est.prontuario,
    est.endereco_bairro,
    est.endereco_logradouro,
    est.endereco_numero,
    est.endereco_complemento,
    est.endereco_cep,
    est.endereco_latitude,
    est.endereco_longitude,
    est.telefone,
    est.fax,
    est.email,
    est.aberto_sempre,
    turno.descricao as turno_atendimento,
    est.diretor_clinico_cpf,
    est.diretor_clinico_conselho,

    -- Metadata
    data_atualizao_registro,
    usuario_atualizador_registro,
    est.data_carga,
    est.data_snapshot,


-- Metadata
from estab_final as est
left join turno using (id_turno_atendimento)
left join unidade using (id_tipo_unidade)

order by ativa desc, est.id_tipo_unidade asc, area_programatica asc, est.endereco_bairro asc, est.nome_fantasia asc
