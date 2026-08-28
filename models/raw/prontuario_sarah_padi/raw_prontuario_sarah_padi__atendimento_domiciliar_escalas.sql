{{
    config(
        alias="atendimento_domiciliar_escalas",
        materialized="incremental",
        unique_key="id_atendimento",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        schema='brutos_prontuario_sarah_padi',
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with source as (
    select
        *
    from {{ source("brutos_prontuario_sarah_padi_staging", "atendimento_domiciliar_escalas") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('atendimento_id') }} as atendimento_id,
        {{ base64_to_string('atendimento_admissao') }} as atendimento_admissao,
        {{ base64_to_string('atendimento_alta') }} as atendimento_alta,
        {{ base64_to_string('alta_no_periodo') }} as alta_no_periodo,
        {{ base64_to_string('admitido_no_periodo') }} as admitido_no_periodo,
        {{ base64_to_string('unidade') }} as unidade,
        {{ base64_to_string('unidade_id') }} as unidade_id,
        {{ base64_to_string('servico') }} as servico,
        {{ base64_to_string('servico_id') }} as servico_id,
        {{ base64_to_string('rota') }} as rota,
        {{ base64_to_string('rota_id') }} as rota_id,
        {{ base64_to_string('paciente_id') }} as paciente_id,
        {{ base64_to_string('paciente_cpf') }} as paciente_cpf,
        {{ base64_to_string('paciente_nome') }} as paciente_nome,
        {{ base64_to_string('paciente_sexo') }} as paciente_sexo,
        {{ base64_to_string('paciente_raca_cor') }} as paciente_raca_cor,
        {{ base64_to_string('paciente_genero_id') }} as paciente_genero_id,
        {{ base64_to_string('paciente_genero') }} as paciente_genero,
        {{ base64_to_string('paciente_dtnasc') }} as paciente_dtnasc,
        {{ base64_to_string('paciente_idade') }} as paciente_idade,
        {{ base64_to_string('bairro') }} as bairro,
        {{ base64_to_string('municipio_uf') }} as municipio_uf,
        {{ base64_to_string('data') }} as data,
        {{ base64_to_string('escala_ordem') }} as escala_ordem,
        {{ base64_to_string('escala_id') }} as escala_id,
        {{ base64_to_string('escala_nome') }} as escala_nome,
        {{ base64_to_string('escala_sigla') }} as escala_sigla,
        {{ base64_to_string('prestador_id') }} as prestador_id,
        {{ base64_to_string('prestador_nome') }} as prestador_nome,
        {{ base64_to_string('prestador_cargo') }} as prestador_cargo,
        {{ base64_to_string('avaliacao') }} as avaliacao,
        {{ base64_to_string('avaliacao_id') }} as avaliacao_id,
        {{ base64_to_string('pontuacao') }} as pontuacao,
        {{ base64_to_string('quantidade') }} as quantidade,
        extracted_at,
        ano_particao,
        mes_particao,
        data_particao
    from source
),

renomeado as (
    select
        safe_cast(atendimento_id as string) as atendimento_id,
        safe_cast(atendimento_admissao as date) as atendimento_admissao,
        safe_cast(atendimento_alta as date) as atendimento_alta,
        safe_cast(alta_no_periodo as string) as alta_no_periodo,
        safe_cast(admitido_no_periodo as string) as admitido_no_periodo,
        safe_cast(unidade as string) as unidade,
        safe_cast(unidade_id as string) as unidade_id,
        safe_cast(servico as string) as servico,
        safe_cast(servico_id as int64) as servico_id,
        safe_cast(rota as string) as rota,
        safe_cast(rota_id as string) as rota_id,
        safe_cast(paciente_id as string) as paciente_id,
        safe_cast(paciente_cpf as string) as paciente_cpf,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_sexo as string) as paciente_sexo,
        safe_cast(paciente_raca_cor as string) as paciente_raca_cor,
        safe_cast(paciente_genero_id as int64) as paciente_genero_id,
        safe_cast(paciente_genero as string) as paciente_genero,
        safe_cast(paciente_dtnasc as date) as paciente_dtnasc,
        safe_cast(paciente_idade as int64) as paciente_idade,
        safe_cast(bairro as string) as bairro,
        safe_cast(municipio_uf as string) as municipio_uf,
        safe_cast(data as date) as data,
        safe_cast(escala_ordem as int64) as escala_ordem,
        safe_cast(escala_id as int64) as escala_id,
        safe_cast(escala_nome as string) as escala_nome,
        safe_cast(escala_sigla as string) as escala_sigla,
        safe_cast(prestador_id as string) as prestador_id,
        safe_cast(prestador_nome as string) as prestador_nome,
        safe_cast(prestador_cargo as string) as prestador_cargo,
        safe_cast(avaliacao as string) as avaliacao,
        safe_cast(avaliacao_id as string) as avaliacao_id,
        safe_cast(pontuacao as int64) as pontuacao,
        safe_cast(quantidade as int64) as quantidade,
        safe_cast(extracted_at as string) as extracted_at,
        safe_cast(ano_particao as string) as ano_particao,
        safe_cast(mes_particao as string) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from base64_para_string
    qualify row_number() over (partition by atendimento_id order by extracted_at desc) = 1
)

select * from renomeado