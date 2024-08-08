{{
    config(
        alias="paciente",
        materialized="incremental",
        unique_key="id",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitacare_staging", "paciente_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by data__id order by source_updated_at desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1)
select
    -- PK
    safe_cast(NULLIF(data__id, '') as string) as id,

    -- Outras Chaves
    safe_cast(NULLIF(patient_cpf, '') as string) as cpf,
    safe_cast(NULLIF(data__dnv, '') as string) as dnv,
    safe_cast(NULLIF(data__nis, '') as string) as nis,
    safe_cast(NULLIF(data__cns, '') as string) as cns,

    -- Informações Pessoais
    safe_cast(NULLIF(data__nome, '') as string) as nome,
    safe_cast(NULLIF(data__nomeSocial, '') as string) as nome_social,
    safe_cast(NULLIF(data__nomeMae, '') as string) as nome_mae,
    safe_cast(NULLIF(data__nomePai, '') as string) as nome_pai,
    safe_cast(NULLIF(data__obito, '') as string) as data_obito,
    safe_cast(NULLIF(data__sexo, '') as string) as sexo,
    safe_cast(NULLIF(data__orientacaoSexual, '') as string) as orientacao_sexual,
    safe_cast(NULLIF(data__identidadeGenero, '') as string) as identidade_genero,
    safe_cast(NULLIF(data__racaCor, '') as string) as raca_cor,

    -- Contato
    safe_cast(NULLIF(data__email, '') as string) as email,
    safe_cast(NULLIF(data__telefone, '') as string) as telefone,

    -- Nascimento
    safe_cast(NULLIF(data__nacionalidade, '') as string) as nacionalidade,
    safe_cast(NULLIF(data__dataNascimento, '') as date) as data_nascimento,
    safe_cast(NULLIF(data__paisNascimento, '') as string) as pais_nascimento,
    safe_cast(NULLIF(data__municipioNascimento, '') as string) as municipio_nascimento,
    safe_cast(NULLIF(data__estadoNascimento, '') as string) as estado_nascimento,

    -- Informações da Unidade
    safe_cast(NULLIF(data__ap, '') as string) as ap,
    safe_cast(NULLIF(data__microarea, '') as string) as microarea,
    safe_cast(NULLIF(payload_cnes, '') as string) as cnes_unidade,
    safe_cast(NULLIF(data__unidade, '') as string) as nome_unidade,
    safe_cast(NULLIF(data__codigoEquipe, '') as string) as codigo_equipe_saude,
    safe_cast(NULLIF(data__ineEquipe, '') as string) as codigo_ine_equipe_saude,
    safe_cast(NULLIF(data__dataAtualizacaoVinculoEquipe, '') as timestamp) as data_atualizacao_vinculo_equipe,
    safe_cast(NULLIF(data__nPront, '') as string) as numero_prontuario,
    safe_cast(NULLIF(data__cadastroPermanente, '') as string) as cadastro_permanente,
    safe_cast(NULLIF(data__dataCadastro, '') as timestamp) as data_cadastro_inicial,
    safe_cast(NULLIF(data__dataAtualizacaoCadastro, '') as timestamp) as data_ultima_atualizacao_cadastral,

    -- Endereço
    safe_cast(NULLIF(data__tipoLogradouro, '') as string) as endereco_tipo_logradouro,
    safe_cast(NULLIF(data__cep, '') as string) as endereco_cep,
    safe_cast(NULLIF(data__logradouro, '') as string) as endereco_logradouro,
    safe_cast(NULLIF(data__bairro, '') as string) as endereco_bairro,
    safe_cast(NULLIF(data__estadoResidencia, '') as string) as endereco_estado,
    safe_cast(NULLIF(data__municipioResidencia, '') as string) as endereco_municipio,

    -- Metadata columns
    safe_cast(NULLIF(data_particao, '') as date) as data_particao,
    safe_cast(NULLIF(source_updated_at, '') as timestamp) as updated_at,
from latest_events

