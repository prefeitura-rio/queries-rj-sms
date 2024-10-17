{{
    config(
        enabled=true,
        schema="saude_cnes",
        alias="estabelecimento_sus_rio_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    -- Obtendo a data mais atual
    versao_atual as (
        select max(data_particao) as versao from {{ ref("raw_cnes_web__tipo_unidade") }}
    ),

    estabelecimentos_brutos as (
        select distinct *
        from {{ ref("int_estabelecimento_sus_rio_historico__brutos_filtrados") }}
    ),

    -- Obtendo atributos dos estabelecimentos via tabela desnormalizada proveniente do
    -- CNES WEB
    estabelecimentos_atributos_cnes_web as (
        select
            id_cnes,
            nome_razao_social,
            nome_fantasia,
            cnpj_mantenedora,
            endereco_bairro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,
            endereco_latitude,
            endereco_longitude,
            id_motivo_desativacao,
            id_unidade,
            aberto_sempre,
            diretor_clinico_cpf,
            diretor_clinico_conselho,
            data_atualizao_registro,
            usuario_atualizador_registro,
            mes_particao,
            ano_particao,
            data_particao,
            data_carga,
            data_snapshot,
            id_distrito_sanitario,
            telefone,
            email
        from {{ ref("raw_cnes_web__estabelecimento") }}
        where data_particao = (select versao from versao_atual)
    ),

    -- Obtendo atributos auxiliares dos estabelecimentos via classificação interna
    -- construída pela SMS
    estabelecimentos_atributos as (
        select
            id_cnes,
            indicador_estabelecimento_sms as estabelecimento_sms_indicador,
            tipo_unidade_subgeral,
            tipo_unidade_agrupado_subgeral,
            esfera_subgeral,
            area_programatica,
            area_programatica_descr,
            agrupador_sms,
            tipo_sms,
            tipo_sms_simplificado,
            nome_limpo,
            nome_sigla,
            prontuario_tem,
            prontuario_versao,
            responsavel_sms,
            administracao,
            prontuario_estoque_tem_dado,
            prontuario_estoque_motivo_sem_dado
        from {{ ref("raw_sheets__estabelecimento_auxiliar") }}
    ),

    -- Obtendo atributos de contato para os estabelecimentos
    contatos_aps as (
        select id_cnes, telefone, email, facebook, instagram, twitter
        from {{ ref("raw_plataforma_smsrio__estabelecimento_contato") }}
    ),

    -- Carregando tabelas utilizadas para mapear códigos em suas descrições textuais
    tp_gestao as (
        select *
        from
            unnest(
                [
                    struct("D" as id_tipo_gestao, "DUPLA" as tipo_gestao_descr),
                    struct("E", "ESTADUAL"),
                    struct("M", "MUNICIPAL"),
                    struct("Z", "SEM GESTAO"),
                    struct("S", "SEM GESTAO"),
                    struct("-Z", "NAO INFORMADO")
                ]
            )
    ),  -- Definição proveniente do CNES FTP DATASUS

    nat_jur as (
        select id_natureza_juridica, descricao as natureza_juridica_descr
        from {{ ref("raw_cnes_web__natureza_juridica") }}
        where data_particao = (select versao from versao_atual)
    ),

    tp_unidade as (
        select id_tipo_unidade, descricao as tipo_unidade_descr
        from {{ ref("raw_cnes_web__tipo_unidade") }}
        where data_particao = (select versao from versao_atual)
    ),

    turno as (
        select id_turno_atendimento, descricao as turno_atendimento
        from {{ ref("raw_cnes_web__turno_atendimento") }}
        where data_particao = (select versao from versao_atual)
    ),

    -- Juntando todos os atributos e mappings, para enriquecer a tabela final de
    -- estabelecimentos
    estabelecimentos_final as (
        select
            -- Brutos FTP DATASUS
            brutos.cep,
            brutos.id_estabelecimento_cnes,
            brutos.id_natureza_juridica,
            brutos.tipo_gestao,
            brutos.tipo_unidade,
            brutos.tipo_turno,
            brutos.vinculo_sus_indicador,
            brutos.atendimento_internacao_sus_indicador,
            brutos.atendimento_ambulatorial_sus_indicador,
            brutos.atendimento_sadt_sus_indicador,
            brutos.atendimento_urgencia_sus_indicador,
            brutos.atendimento_outros_sus_indicador,
            brutos.atendimento_vigilancia_sus_indicador,
            brutos.atendimento_regulacao_sus_indicador,

            -- CNES Web
            cnes_web.nome_razao_social,
            cnes_web.nome_fantasia,
            cnes_web.cnpj_mantenedora,
            cnes_web.endereco_bairro,
            cnes_web.endereco_logradouro,
            cnes_web.endereco_numero,
            cnes_web.endereco_complemento,
            cnes_web.endereco_latitude,
            cnes_web.endereco_longitude,
            cnes_web.id_motivo_desativacao,
            cnes_web.id_unidade,
            cnes_web.aberto_sempre,
            cnes_web.diretor_clinico_cpf,
            cnes_web.diretor_clinico_conselho,
            cnes_web.data_atualizao_registro,
            cnes_web.usuario_atualizador_registro,
            cnes_web.mes_particao,
            cnes_web.ano_particao,
            cnes_web.data_particao,
            cnes_web.data_carga,
            cnes_web.data_snapshot,
            cnes_web.email as email_cnes,
            split(cnes_web.telefone, "/") as telefone_cnes,

            -- Atributos criados in house
            estabelecimentos_atributos.estabelecimento_sms_indicador,
            estabelecimentos_atributos.tipo_unidade_subgeral
            as tipo_unidade_alternativo,
            estabelecimentos_atributos.tipo_unidade_agrupado_subgeral
            as tipo_unidade_agrupado,
            estabelecimentos_atributos.esfera_subgeral as esfera,
            estabelecimentos_atributos.area_programatica as id_ap,
            estabelecimentos_atributos.area_programatica_descr as ap,
            estabelecimentos_atributos.agrupador_sms,
            estabelecimentos_atributos.tipo_sms,
            estabelecimentos_atributos.tipo_sms_simplificado,
            estabelecimentos_atributos.nome_limpo,
            estabelecimentos_atributos.nome_sigla,
            estabelecimentos_atributos.prontuario_tem,
            estabelecimentos_atributos.prontuario_versao,
            estabelecimentos_atributos.responsavel_sms,
            estabelecimentos_atributos.administracao,
            estabelecimentos_atributos.prontuario_estoque_tem_dado,
            estabelecimentos_atributos.prontuario_estoque_motivo_sem_dado,
            regexp_replace(
                estabelecimentos_atributos.nome_limpo,
                r'(CF |CSE |CMS |UPA 24h |POLICLINICA |HOSPITAL MUNICIPAL |COORD DE EMERGENCIA REGIONAL CER |MATERNIDADE )',
                ''
            ) as nome_complemento,

            -- Mappings oficiais do CNES / DATASUS
            tp_gestao.tipo_gestao_descr,
            nat_jur.natureza_juridica_descr,
            tp_unidade.tipo_unidade_descr,
            turno.turno_atendimento,

            -- Informações de Contato
            contatos_aps.telefone as telefone_aps,
            contatos_aps.facebook,
            contatos_aps.instagram,
            contatos_aps.twitter,
            contatos_aps.email as email_aps,

            -- Metadados
            brutos.ano,
            brutos.mes,

        from estabelecimentos_brutos as brutos
        left join
            estabelecimentos_atributos_cnes_web as cnes_web
            on cast(brutos.id_estabelecimento_cnes as int64)
            = cast(cnes_web.id_cnes as int64)
        left join
            nat_jur
            on cast(brutos.id_natureza_juridica as int64)
            = cast(nat_jur.id_natureza_juridica as int64)
        left join
            tp_unidade
            on cast(brutos.tipo_unidade as int64)
            = cast(tp_unidade.id_tipo_unidade as int64)
        left join
            turno
            on cast(brutos.tipo_turno as int64)
            = cast(turno.id_turno_atendimento as int64)
        left join tp_gestao on brutos.tipo_gestao = tp_gestao.id_tipo_gestao
        left join
            estabelecimentos_atributos
            on cast(brutos.id_estabelecimento_cnes as int64)
            = cast(estabelecimentos_atributos.id_cnes as int64)
        left join
            contatos_aps
            on cast(brutos.id_estabelecimento_cnes as int64)
            = cast(contatos_aps.id_cnes as int64)
    ),

    -- Seleção final
    final as (
        select
            -- Identificação
            lpad(id_estabelecimento_cnes, 7, '0') as id_cnes,
            id_unidade,
            nome_razao_social,
            nome_fantasia,
            nome_limpo,
            nome_sigla,
            nome_complemento,
            lpad(cnpj_mantenedora, 14, '0') as cnpj_mantenedora,

            -- Responsabilização
            esfera,
            cast(id_natureza_juridica as int64) as id_natureza_juridica,
            natureza_juridica_descr,
            tipo_gestao,
            tipo_gestao_descr,
            responsavel_sms,
            administracao,
            lpad(diretor_clinico_cpf, 11, '0') as diretor_clinico_cpf,
            diretor_clinico_conselho,

            -- Atributos
            cast(tipo_turno as int64) as tipo_turno,
            turno_atendimento,
            aberto_sempre,

            -- Tipagem dos Estabelecimentos (CNES)
            cast(tipo_unidade as int64) as id_tipo_unidade,
            tipo_unidade_descr as tipo,  -- Renomear para tipo_cnes?

            -- Tipagem dos Estabelecimentos (DIT)
            tipo_sms,
            tipo_sms_simplificado,
            agrupador_sms as tipo_sms_agrupado,

            -- Tipagem dos Estabelecimentos (SUBGERAL)
            tipo_unidade_alternativo,
            tipo_unidade_agrupado,

            -- Localização
            id_ap,
            ap,
            lpad(cep, 8, '0') as endereco_cep,
            endereco_bairro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,
            endereco_latitude,
            endereco_longitude,

            -- Status
            case
                when id_motivo_desativacao is null or id_motivo_desativacao = ''
                then 'sim'
                else 'não'
            end as ativa,

            -- Prontuário
            prontuario_tem,
            prontuario_versao,
            prontuario_estoque_tem_dado,
            prontuario_estoque_motivo_sem_dado,

            -- Informações de contato
            coalesce(telefone_aps, telefone_cnes) as telefone,
            coalesce(email_aps, email_cnes) as email,
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
            cast(ano as int64) as ano_competencia,
            cast(mes as int64) as mes_competencia,
            --safe_cast(mes_particao as int64) as mes_particao,
            --safe_cast(ano_particao as int64) as ano_particao,
            parse_date('%Y-%m-%d', data_particao) as data_particao,
            data_carga,
            data_snapshot

        from estabelecimentos_final
        order by ano_competencia asc, mes_competencia asc, id_cnes asc
    )

select *
from final
