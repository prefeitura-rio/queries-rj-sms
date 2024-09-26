{{
    config(
        schema="saude_cnes",
        alias="leito_sus_rio_historico"
    )
}}

with

versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
), 

leitos_mapping_cnesftp as (
    select * from unnest([
        STRUCT(1 as tipo_leito, "CIRURGICO" as tipo_leito_descr),
        STRUCT(2, "CLINICO"),
        STRUCT(3, "COMPLEMENTAR"),
        STRUCT(4, "OBSTETRICO"),
        STRUCT(5, "PEDIATRICO"),
        STRUCT(6, "OUTROS"),
        STRUCT(7, "HOSPITAL / DIA")
    ])
),

leitos_mapping_cnesweb as (
    select
        distinct id_leito_especialidade as tipo_especialidade_leito,
        leito_especialidade as tipo_especialidade_leito_descr
    from {{ ref("raw_cnes_web__leito") }}
),

estabelecimentos_mrj_sus as (
    select * from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

leitos_mrj_sus as (
    select 
        lt.tipo_leito,
        lt.tipo_especialidade_leito,
        lt.quantidade_total,
        lt.quantidade_contratado,
        lt.quantidade_sus,

        ftp.tipo_leito_descr,
        web.tipo_especialidade_leito_descr,
        estabs.*

    from  {{ ref("raw_cnes_ftp__leito") }} as lt
    left join leitos_mapping_cnesftp as ftp on safe_cast(lt.tipo_leito as int64) = ftp.tipo_leito
    left join leitos_mapping_cnesweb as web on safe_cast(lt.tipo_especialidade_leito as int64) = safe_cast(web.tipo_especialidade_leito as int64)
    left join estabelecimentos_mrj_sus as estabs on lt.ano = estabs.ano and lt.mes = estabs.mes and safe_cast(lt.id_estabelecimento_cnes as int64) = safe_cast(estabs.id_cnes as int64)
    
    where lt.ano >= 2008 and safe_cast(lt.id_estabelecimento_cnes as int64) in (select distinct safe_cast(id_cnes as int64) from estabelecimentos_mrj_sus)
),

final as (
    select 
        --------------------------------- ESTABELECIMENTOS
        -- Identificação
        ano,
        mes,
        id_cnes,
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
        atendimento_regulacao_sus_indicador,

        -- Metadados
        data_atualizao_registro,
        usuario_atualizador_registro,
        mes_particao,
        ano_particao,
        data_particao,
        data_carga,
        data_snapshot,

        --------------------------------- LEITOS
        tipo_leito,
        tipo_leito_descr,
        tipo_especialidade_leito,
        tipo_especialidade_leito_descr,
        quantidade_total,
        quantidade_contratado,
        quantidade_sus

    from leitos_mrj_sus
    where id_cnes is not null
    order by ano asc, mes asc, id_cnes asc, tipo_leito_descr asc, tipo_especialidade_leito_descr asc
)

select * from final