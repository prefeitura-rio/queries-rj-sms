{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_paciente_continuo",
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key="id",
        tags=['daily']
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with

    source as (
        select *, 
            concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id,
            greatest(safe_cast(source_updated_at as timestamp),
            safe_cast(json_extract_scalar(data, "$.dataCadastro") as timestamp),
            safe_cast(json_extract_scalar(data, "$.dataAtualizacaoCadastro") as timestamp),
            safe_cast(json_extract_scalar(data, "$.dataAtualizacaoVinculoEquipe") as timestamp),
            safe_cast(nullif(json_extract_scalar(data, "$.dataCadastro"),'') as timestamp)
            ) as updated_at_rank
        from {{ source("brutos_prontuario_vitacare_staging", "paciente_continuo") }}
        where {{process_null('payload_cnes')}} is not null
        {% if is_incremental() %}
        and TIMESTAMP_TRUNC(datalake_loaded_at, DAY) > TIMESTAMP(date_sub(current_date('America/Sao_Paulo'), interval 30 day))
        {% endif %}
    ),
    
    latest_events as (
        select
            *
        from source
        where {{process_null('payload_cnes')}} is not null
        qualify 
            row_number() over (partition by id order by updated_at_rank desc) = 1
        
    ),

    paciente_continuo as (
        select
            -- PK
            cast(id as string) as id,

            -- Outras chaves
            json_extract_scalar(data, "$.cnes") as id_cnes,
            json_extract_scalar(data, "$.id") as id_local,
            json_extract_scalar(data, "$.nPront") as numero_prontuario,
            json_extract_scalar(data, "$.cpf") as cpf,
            json_extract_scalar(data, "$.dnv") as dnv,
            json_extract_scalar(data, "$.nis") as nis,
            json_extract_scalar(data, "$.cns") as cns,

            -- Informações Pessoais
            json_extract_scalar(data, "$.nome") as nome,
            json_extract_scalar(data, "$.nomeSocial") as nome_social,
            json_extract_scalar(data, "$.nomeMae") as nome_mae,
            json_extract_scalar(data, "$.nomePai") as nome_pai, 
            case           
                when json_extract_scalar(data, "$.obito") = 'true' then true
                when json_extract_scalar(data, "$.obito") = 'false' then false
                else null
            end as obito,
            json_extract_scalar(data, "$.sexo") as sexo,
            json_extract_scalar(data, "$.orientacaoSexual") as orientacao_sexual,
            json_extract_scalar(data, "$.identidadeGenero") as identidade_genero,
            json_extract_scalar(data, "$.racaCor") as raca_cor,

            -- Informações Cadastrais
            safe_cast(null as string) as situacao,  -- #TODO: Pedir para vitacare essa informação
            case 
                when json_extract_scalar(data, "$.cadastroPermanente") = 'true' then true 
                when json_extract_scalar(data, "$.cadastroPermanente") = 'false' then false 
                else null 
            end as cadastro_permanente,
            safe_cast(json_extract_scalar(data, "$.dataCadastro") as timestamp) as data_cadastro,
            safe_cast(json_extract_scalar(data, "$.dataAtualizacaoCadastro") as timestamp) as data_atualizacao_cadastro,
            
            -- Nascimento
            json_extract_scalar(data, "$.nacionalidade") as nacionalidade,
            safe_cast( 
                safe_cast(json_extract_scalar(data, "$.dataNascimento") as datetime)
                as date
            ) as data_nascimento,
            json_extract_scalar(data, "$.paisNascimento") as pais_nascimento,
            json_extract_scalar(data, "$.municipioNascimento") as municipio_nascimento,
            json_extract_scalar(data, "$.estadoNascimento") as estado_nascimento,

            -- Contato
            json_extract_scalar(data, "$.email") as email,
            json_extract_scalar(data, "$.telefone") as telefone,

            -- Endereço
            safe_cast(null as string) as endereco_tipo_domicilio,
            json_extract_scalar(data, "$.tipoLogradouro") as endereco_tipo_logradouro,
            json_extract_scalar(data, "$.cep") as endereco_cep,
            json_extract_scalar(data, "$.logradouro") as endereco_logradouro,
            json_extract_scalar(data, "$.bairro") as endereco_bairro,
            json_extract_scalar(data, "$.estadoResidencia") as endereco_estado,
            json_extract_scalar(data, "$.municipioResidencia") as endereco_municipio,

            -- Informações da Unidade
            json_extract_scalar(data, "$.ap") as ap,
            json_extract_scalar(data, "$.microarea") as microarea,
            json_extract_scalar(data, "$.unidade") as nome_unidade,
            json_extract_scalar(data, "$.codigoEquipe") as codigo_equipe_saude,
            json_extract_scalar(data, "$.ineEquipe") as codigo_ine_equipe_saude,
            safe_cast(json_extract_scalar(data, "$.dataAtualizacaoVinculoEquipe") as timestamp) as data_atualizacao_vinculo_equipe,
            
            -- Metadata columns
            safe_cast(
                safe_cast(json_extract_scalar(data, "$.dataCadastro") as datetime)
                as date) as data_particao,
            safe_cast(nullif(json_extract_scalar(data, "$.dataCadastro"),'') as timestamp) as source_created_at,
            updated_at_rank as source_updated_at,
            safe_cast(null as timestamp) as datalake_imported_at,
            updated_at_rank
        from latest_events
    )
select * 
from paciente_continuo
    