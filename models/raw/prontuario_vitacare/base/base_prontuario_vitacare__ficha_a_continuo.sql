{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_ficha_a_continuo",
        materialized="incremental",
        unique_key="id",
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with

    source as (
        select *, 
                concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id
            from {{ source("brutos_prontuario_vitacare_staging", "paciente_continuo") }}
            {% if is_incremental() %} where source_updated_at > '{{seven_days_ago}}' {% endif %}
    ),


    latest_events as (
        select
            *
        from source
        qualify 
            row_number() over (partition by id order by source_updated_at desc) = 1   
    ),

    ficha_a_continuo as (
        select
            -- PK
            cast(id as string) as id,
            
            nullif(JSON_EXTRACT_SCALAR(data, '$.cpf'), '') AS cpf,
            nullif(JSON_EXTRACT_SCALAR(data, '$.id'), '') AS id_paciente,
            nullif(JSON_EXTRACT_SCALAR(data, '$.nPront'), '') AS numero_prontuario,

            nullif(JSON_EXTRACT_SCALAR(data, '$.cnes'), '') AS unidade_cadastro,
            nullif(JSON_EXTRACT_SCALAR(data, '$.ap'), '') AS ap_cadastro,

            nullif(JSON_EXTRACT_SCALAR(data, '$.nome'), '') AS nome,
            nullif(JSON_EXTRACT_SCALAR(data, '$.sexo'), '') AS sexo,
            case           
                when nullif(json_extract_scalar(data, "$.obito"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.obito"), '') = 'false' then false
                else null
            end as obito,
            nullif(JSON_EXTRACT_SCALAR(data, '$.bairro'), '') AS bairro,
            nullif(JSON_EXTRACT_SCALAR(data, '$.comodos'), '') AS comodos,
            nullif(JSON_EXTRACT_SCALAR(data, '$.nomeMae'), '') AS nome_mae,
            nullif(JSON_EXTRACT_SCALAR(data, '$.nomePai'), '') AS nome_pai,
            nullif(JSON_EXTRACT_SCALAR(data, '$.racaCor'), '') AS raca_cor,
            nullif(JSON_EXTRACT_SCALAR(data, '$.ocupacao'), '') AS ocupacao,
            nullif(JSON_EXTRACT_SCALAR(data, '$.religiao'), '') AS religiao,
            nullif(JSON_EXTRACT_SCALAR(data, '$.telefone'), '') AS telefone,
            nullif(JSON_EXTRACT_SCALAR(data, '$.ineEquipe'), '') AS ine_equipe,
            nullif(JSON_EXTRACT_SCALAR(data, '$.microarea'), '') AS microarea,
            nullif(JSON_EXTRACT_SCALAR(data, '$.logradouro'), '') AS logradouro,
            nullif(JSON_EXTRACT_SCALAR(data, '$.nomeSocial'), '') AS nome_social,
            nullif(JSON_EXTRACT_SCALAR(data, '$.destinoLixo'), '') AS destino_lixo,
            case           
                when nullif(json_extract_scalar(data, "$.luzEletrica"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.luzEletrica"), '') = 'false' then false
                else null
            end as luz_eletrica,
            nullif(JSON_EXTRACT_SCALAR(data, '$.codigoEquipe'), '') AS codigo_equipe,
            nullif(JSON_EXTRACT_SCALAR(data, '$.dataCadastro'), '') AS data_cadastro,
            nullif(JSON_EXTRACT_SCALAR(data, '$.escolaridade'), '') AS escolaridade,
            nullif(JSON_EXTRACT_SCALAR(data, '$.tempoMoradia'), '') AS tempo_moradia,
            nullif(JSON_EXTRACT_SCALAR(data, '$.nacionalidade'), '') AS nacionalidade,
            nullif(JSON_EXTRACT_SCALAR(data, '$.rendaFamiliar'), '') AS renda_familiar,
            nullif(JSON_EXTRACT_SCALAR(data, '$.tipoDomicilio'), '') AS tipo_domicilio,
            safe_cast(
                safe_cast(nullif(JSON_EXTRACT_SCALAR(data, '$.dataNascimento'), '') as datetime) as date) AS data_nascimento,
            nullif(JSON_EXTRACT_SCALAR(data, '$.paisNascimento'), '') AS pais_nascimento,
            nullif(JSON_EXTRACT_SCALAR(data, '$.tipoLogradouro'), '') AS tipo_logradouro,
            nullif(JSON_EXTRACT_SCALAR(data, '$.tratamentoAgua'), '') AS tratamento_agua,
            case           
                when nullif(json_extract_scalar(data, "$.emSituacaoDeRua"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.emSituacaoDeRua"), '') = 'false' then false
                else null
            end as em_situacao_de_rua,
            case           
                when nullif(json_extract_scalar(data, "$.frequentaEscola"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.frequentaEscola"), '') = 'false' then false
                else null
            end as frequenta_escola,
            nullif(ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.meiosTransporte'), ', '), '') AS meios_transporte,
            nullif(JSON_EXTRACT_SCALAR(data, '$.situacaoUsuario'), '') AS situacao_usuario,
            nullif(ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.doencasCondicoes'), ', '), '') as doencas_condicoes,
            nullif(JSON_EXTRACT_SCALAR(data, '$.estadoNascimento'), '') AS estado_nascimento,
            nullif(JSON_EXTRACT_SCALAR(data, '$.estadoResidencia'), '') AS estado_residencia,
            nullif(JSON_EXTRACT_SCALAR(data, '$.identidadeGenero'), '') AS identidade_genero,
            nullif(ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.meiosComunicacao'), ', '), '') AS meios_comunicacao,
            nullif(JSON_EXTRACT_SCALAR(data, '$.orientacaoSexual'), '') AS orientacao_sexual,
            case           
                when nullif(json_extract_scalar(data, "$.possuiFiltroAgua"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.possuiFiltroAgua"), '') = 'false' then false
                else null
            end as possui_filtro_agua,
            case           
                when nullif(json_extract_scalar(data, "$.possuiPlanoSaude"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.possuiPlanoSaude"), '') = 'false' then false
                else null
            end as possui_plano_saude,
            nullif(JSON_EXTRACT_SCALAR(data, '$.situacaoFamiliar'), '') AS situacao_familiar,
            case           
                when nullif(json_extract_scalar(data, "$.territorioSocial"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.territorioSocial"), '') = 'false' then false
                else null
            end as territorio_social,
            nullif(JSON_EXTRACT_SCALAR(data, '$.abastecimentoAgua'), '') AS abastecimento_agua,
            case           
                when nullif(json_extract_scalar(data, "$.animaisNoDomicilio"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.animaisNoDomicilio"), '') = 'false' then false
                else null
            end as animais_no_domicilio,
            case           
                when nullif(json_extract_scalar(data, "$.cadastroPermanente"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.cadastroPermanente"), '') = 'false' then false
                else null
            end as cadastro_permanente,
            nullif(JSON_EXTRACT_SCALAR(data, '$.familiaLocalizacao'), '') AS familia_localizacao,
            nullif(ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.emCasoDoencaProcura'), ', '), '') AS em_caso_doenca_procura,
            nullif(JSON_EXTRACT_SCALAR(data, '$.municipioNascimento'), '') AS municipio_nascimento,
            nullif(JSON_EXTRACT_SCALAR(data, '$.municipioResidencia'), '') AS municipio_residencia,
            case           
                when nullif(json_extract_scalar(data, "$.responsavelFamiliar"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.responsavelFamiliar"), '') = 'false' then false
                else null
            end as responsavel_familiar,
            nullif(JSON_EXTRACT_SCALAR(data, '$.esgotamentoSanitario'), '') AS esgotamento_sanitario,
            nullif(JSON_EXTRACT_SCALAR(data, '$.situacaoMoradiaPosse'), '') AS situacao_moradia_posse,
            nullif(JSON_EXTRACT_SCALAR(data, '$.situacaoProfissional'), '') AS situacao_profissional,
            case           
                when nullif(json_extract_scalar(data, "$.vulnerabilidadeSocial"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.vulnerabilidadeSocial"), '') = 'false' then false
                else null
            end as vulnerabilidade_social,
            case           
                when nullif(json_extract_scalar(data, "$.familiaBeneficiariaCfc"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.familiaBeneficiariaCfc"), '') = 'false' then false
                else null
            end as familia_beneficiaria_cfc,
            nullif(JSON_EXTRACT_SCALAR(data, '$.dataAtualizacaoCadastro'), '') AS data_atualizacao_cadastro,
            case           
                when nullif(json_extract_scalar(data, "$.participaGrupoComunitario"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.participaGrupoComunitario"), '') = 'false' then false
                else null
            end as participa_grupo_comunitario,
            nullif(JSON_EXTRACT_SCALAR(data, '$.relacaoResponsavelFamiliar'), '') AS relacao_responsavel_familiar,
            case           
                when nullif(json_extract_scalar(data, "$.membroComunidadeTradicional"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.membroComunidadeTradicional"), '') = 'false' then false
                else null
            end as membro_comunidade_tradicional,
            nullif(JSON_EXTRACT_SCALAR(data, '$.dataAtualizacaoVinculoEquipe'), '') AS data_atualizacao_vinculo_equipe,
            case           
                when nullif(json_extract_scalar(data, "$.familiaBeneficiariaAuxilioBrasil"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.familiaBeneficiariaAuxilioBrasil"), '') = 'false' then false
                else null
            end as familia_beneficiaria_auxilio_brasil,
            case           
                when nullif(json_extract_scalar(data, "$.criancaMatriculadaCrechePreEscola"), '') = 'true' then true
                when nullif(json_extract_scalar(data, "$.criancaMatriculadaCrechePreEscola"), '') = 'false' then false
                else null
            end as crianca_matriculada_creche_pre_escola,

            source_updated_at as updated_at,
            cast(datalake_loaded_at as string) as loaded_at

        from latest_events
    )

select * from ficha_a_continuo