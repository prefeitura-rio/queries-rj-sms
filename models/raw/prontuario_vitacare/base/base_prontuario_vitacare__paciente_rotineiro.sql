{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_paciente_rotineiro",
        materialized="incremental",
        unique_key="id",
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with

    events_from_window as (
        select *, concat(nullif(payload_cnes, ''), '.', nullif(data__id, '')) as id
        from {{ source("brutos_prontuario_vitacare_staging", "paciente_eventos_cloned") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),

    events_ranked_by_freshness as (
        select
            *,
            row_number() over (partition by id order by source_updated_at desc) as rank
        from events_from_window
    ),

    latest_events as (select * from events_ranked_by_freshness where rank = 1)

select
    -- PK
    safe_cast(id as string) as id,

    -- Outras Chaves
    safe_cast(nullif(payload_cnes, '') as string) as id_cnes,
    safe_cast(nullif(data__id, '') as string) as id_local,
    safe_cast(nullif(data__npront, '') as string) as numero_prontuario,
    safe_cast(nullif(patient_cpf, '') as string) as cpf,
    safe_cast(nullif(data__dnv, '') as string) as dnv,
    safe_cast(nullif(data__nis, '') as string) as nis,
    safe_cast(nullif(data__cns, '') as string) as cns,

    -- Informações Pessoais
    safe_cast(nullif(data__nome, '') as string) as nome,
    safe_cast(nullif(data__nomesocial, '') as string) as nome_social,
    safe_cast(nullif(data__nomemae, '') as string) as nome_mae,
    safe_cast(nullif(data__nomepai, '') as string) as nome_pai,
    safe_cast(nullif(data__obito, '') as string) as obito,
    safe_cast(nullif(lower(data__sexo), '') as string) as sexo,
    safe_cast(nullif(lower(data__orientacaosexual), '') as string) as orientacao_sexual,
    safe_cast(nullif(lower(data__identidadegenero), '') as string) as identidade_genero,
    safe_cast(nullif(lower(data__racacor), '') as string) as raca_cor,

    -- Informações Cadastrais
    safe_cast(null as string) as situacao,  -- #TODO: Pedir para vitacare essa informação
    safe_cast(nullif(data__cadastropermanente, '') as string) as cadastro_permanente,
    safe_cast(nullif(data__datacadastro, '') as timestamp) as data_cadastro_inicial,
    safe_cast(
        nullif(data__dataatualizacaocadastro, '') as timestamp
    ) as data_ultima_atualizacao_cadastral,

    -- Nascimento
    safe_cast(nullif(data__nacionalidade, '') as string) as nacionalidade,
    safe_cast(nullif(data__datanascimento, '') as date) as data_nascimento,
    safe_cast(nullif(data__paisnascimento, '') as string) as pais_nascimento,
    safe_cast(nullif(data__municipionascimento, '') as string) as municipio_nascimento,
    safe_cast(nullif(data__estadonascimento, '') as string) as estado_nascimento,

    -- Contato
    safe_cast(nullif(data__email, '') as string) as email,
    safe_cast(nullif(data__telefone, '') as string) as telefone,

    -- Endereço
    safe_cast(null as string) as endereco_tipo_domicilio,
    safe_cast(nullif(data__tipologradouro, '') as string) as endereco_tipo_logradouro,
    safe_cast(nullif(data__cep, '') as string) as endereco_cep,
    safe_cast(nullif(data__logradouro, '') as string) as endereco_logradouro,
    safe_cast(nullif(data__bairro, '') as string) as endereco_bairro,
    safe_cast(nullif(data__estadoresidencia, '') as string) as endereco_estado,
    safe_cast(nullif(data__municipioresidencia, '') as string) as endereco_municipio,

    -- Informações da Unidade
    safe_cast(nullif(data__ap, '') as string) as ap,
    safe_cast(nullif(data__microarea, '') as string) as microarea,
    safe_cast(nullif(data__unidade, '') as string) as nome_unidade,
    safe_cast(nullif(data__codigoequipe, '') as string) as codigo_equipe_saude,
    safe_cast(nullif(data__ineequipe, '') as string) as codigo_ine_equipe_saude,
    safe_cast(
        nullif(data__dataatualizacaovinculoequipe, '') as timestamp
    ) as data_atualizacao_vinculo_equipe,

    -- Metadata columns
    safe_cast(nullif(data_particao, '') as date) as data_particao,
    safe_cast(nullif(data__dataCadastro, '') as timestamp) as source_created_at,
    safe_cast(nullif(source_updated_at, '') as timestamp) as source_updated_at,
    safe_cast(null as timestamp) as datalake_imported_at,

from latest_events
