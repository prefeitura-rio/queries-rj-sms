{{
    config(
        alias="atendimento_domiciliar_saidas",
        materialized="incremental",
        unique_key="id_registro",
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
    from {{ source("brutos_prontuario_sarah_padi_staging", "atendimento_domiciliar_saidas") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('registro_id') }} as registro_id,
        {{ base64_to_string('registro_data') }} as registro_data,
        {{ base64_to_string('captacao') }} as captacao,
        {{ base64_to_string('captacao_data') }} as captacao_data,
        {{ base64_to_string('paciente_id') }} as paciente_id,
        {{ base64_to_string('paciente_cpf') }} as paciente_cpf,
        {{ base64_to_string('paciente_nome') }} as paciente_nome,
        {{ base64_to_string('paciente_sexo') }} as paciente_sexo,
        {{ base64_to_string('paciente_raca_cor') }} as paciente_raca_cor,
        {{ base64_to_string('paciente_genero_id') }} as paciente_genero_id,
        {{ base64_to_string('paciente_genero') }} as paciente_genero,
        {{ base64_to_string('paciente_dtnasc') }} as paciente_dtnasc,
        {{ base64_to_string('paciente_idade') }} as paciente_idade,
        {{ base64_to_string('faixa_etaria_id') }} as faixa_etaria_id,
        {{ base64_to_string('faixa_etaria') }} as faixa_etaria,
        {{ base64_to_string('municipio_uf') }} as municipio_uf,
        {{ base64_to_string('endereco') }} as endereco,
        {{ base64_to_string('contato') }} as contato,
        {{ base64_to_string('origem') }} as origem,
        {{ base64_to_string('origem_id') }} as origem_id,
        {{ base64_to_string('demanda_judicial') }} as demanda_judicial,
        {{ base64_to_string('sisreg_numero') }} as sisreg_numero,
        {{ base64_to_string('rota') }} as rota,
        {{ base64_to_string('rota_id') }} as rota_id,
        {{ base64_to_string('bairro') }} as bairro,
        {{ base64_to_string('clinica') }} as clinica,
        {{ base64_to_string('clinica_id') }} as clinica_id,
        {{ base64_to_string('especialidade') }} as especialidade,
        {{ base64_to_string('especialidade_id') }} as especialidade_id,
        {{ base64_to_string('dados_clinicos') }} as dados_clinicos,
        {{ base64_to_string('dados_clinicos_data') }} as dados_clinicos_data,
        {{ base64_to_string('dados_clinicos_dias') }} as dados_clinicos_dias,
        {{ base64_to_string('dados_clinicos_prof') }} as dados_clinicos_prof,
        {{ base64_to_string('dados_clinicos_cargo') }} as dados_clinicos_cargo,
        {{ base64_to_string('cids') }} as cids,
        {{ base64_to_string('avd') }} as avd,
        {{ base64_to_string('avd_data') }} as avd_data,
        {{ base64_to_string('avd_dias') }} as avd_dias,
        {{ base64_to_string('avd_elegivel') }} as avd_elegivel,
        {{ base64_to_string('avd_modalidade') }} as avd_modalidade,
        {{ base64_to_string('avd_motivo') }} as avd_motivo,
        {{ base64_to_string('avd_motivo_detalhe') }} as avd_motivo_detalhe,
        {{ base64_to_string('avd_prof') }} as avd_prof,
        {{ base64_to_string('avd_cargo') }} as avd_cargo,
        {{ base64_to_string('respiracao') }} as respiracao,
        {{ base64_to_string('interface') }} as interface,
        {{ base64_to_string('modo_ventilatorio') }} as modo_ventilatorio,
        {{ base64_to_string('dispositivo_invasivo') }} as dispositivo_invasivo,
        {{ base64_to_string('spict_br') }} as spict_br,
        {{ base64_to_string('iaec_ad') }} as iaec_ad,
        {{ base64_to_string('linha_cuidado') }} as linha_cuidado,
        {{ base64_to_string('cuidado_paliativo') }} as cuidado_paliativo,
        {{ base64_to_string('barthel_inicial') }} as barthel_inicial,
        {{ base64_to_string('barthel_final') }} as barthel_final,
        {{ base64_to_string('unidade') }} as unidade,
        {{ base64_to_string('unidade_id') }} as unidade_id,
        {{ base64_to_string('unidade_origem') }} as unidade_origem,
        {{ base64_to_string('servico') }} as servico,
        {{ base64_to_string('servico_id') }} as servico_id,
        {{ base64_to_string('registro_alta') }} as registro_alta,
        {{ base64_to_string('motivo') }} as motivo,
        {{ base64_to_string('motivo_id') }} as motivo_id,
        {{ base64_to_string('destino') }} as destino,
        {{ base64_to_string('destino_id') }} as destino_id,
        {{ base64_to_string('tipo_destino') }} as tipo_destino,
        {{ base64_to_string('tipo_destino_id') }} as tipo_destino_id,
        {{ base64_to_string('permanencia') }} as permanencia,
        {{ base64_to_string('quantidade') }} as quantidade,
        extracted_at,
        ano_particao,
        mes_particao,
        data_particao
    from source
),

renomeado as (
    select
        safe_cast(registro_id as string) as id_registro,
        date(registro_data) as registro_data,
        safe_cast(captacao as string) as captacao,
        date(captacao_data) as captacao_data,

        -- Dados do paciente
        safe_cast(paciente_id as string) as id_paciente,
        safe_cast(paciente_cpf as string) as paciente_cpf,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_sexo as string) as paciente_sexo,
        safe_cast(paciente_raca_cor as string) as paciente_raca_cor,
        safe_cast(paciente_genero_id as int64) as id_paciente_genero,
        safe_cast(paciente_genero as string) as paciente_genero,
        date(paciente_dtnasc) as paciente_nascimento_data,
        safe_cast(paciente_idade as int64) as paciente_idade,
        safe_cast(faixa_etaria_id as int64) as paciente_id_faixa_etaria,
        safe_cast(faixa_etaria as string) as paciente_faixa_etaria,
        safe_cast(municipio_uf as string) as paciente_municipio_uf,
        safe_cast(bairro as string) as paciente_bairro,
        safe_cast(endereco as string) as paciente_endereco,
        safe_cast(contato as string) as paciente_contato,

        -- Encaminhamento
        safe_cast(origem as string) as origem,
        safe_cast(origem_id as int64) as id_origem,
        safe_cast(demanda_judicial as string) as demanda_judicial,
        safe_cast(sisreg_numero as string) as sisreg_numero,
        safe_cast(rota as string) as rota,
        safe_cast(rota_id as string) as rota_id,
        safe_cast(clinica as string) as clinica,
        safe_cast(clinica_id as int64) as id_clinica,
        safe_cast(especialidade as string) as especialidade,
        safe_cast(especialidade_id as int64) as id_especialidade,
        safe_cast(dados_clinicos as string) as dados_clinicos,
        date(dados_clinicos_data) as dados_clinicos_data,
        safe_cast(dados_clinicos_dias as int64) as dados_clinicos_dias,
        safe_cast(dados_clinicos_prof as string) as dados_clinicos_profissional,
        safe_cast(dados_clinicos_cargo as string) as dados_clinicos_cargo,
        safe_cast(cids as string) as cids,

        -- AVD
        safe_cast(avd as string) as avd,
        date(avd_data) as avd_data,
        safe_cast(avd_dias as int64) as avd_dias,
        safe_cast(avd_elegivel as string) as avd_elegivel,
        safe_cast(avd_modalidade as string) as avd_modalidade,
        safe_cast(avd_motivo as string) as avd_motivo,
        safe_cast(avd_motivo_detalhe as string) as avd_motivo_detalhe,
        safe_cast(avd_prof as string) as avd_profissional,
        safe_cast(avd_cargo as string) as avd_cargo,
        safe_cast(respiracao as string) as respiracao,
        safe_cast(interface as string) as interface,
        safe_cast(modo_ventilatorio as string) as modo_ventilatorio,
        safe_cast(dispositivo_invasivo as string) as dispositivo_invasivo,
        safe_cast(spict_br as int64) as spict_br,
        safe_cast(iaec_ad as int64) as iaec_ad,

        -- Linha de cuidado
        safe_cast(linha_cuidado as string) as linha_cuidado,
        safe_cast(cuidado_paliativo as string) as cuidado_paliativo,
        safe_cast(barthel_inicial as int64) as barthel_inicial,
        safe_cast(barthel_final as int64) as barthel_final,

        -- Serviço
        safe_cast(unidade as string) as unidade_nome,
        safe_cast(unidade_id as string) as id_unidade,
        safe_cast(unidade_origem as string) as unidade_origem,
        safe_cast(servico as string) as servico,
        safe_cast(servico_id as int64) as id_servico,

        -- Saída
        date(registro_alta) as registro_alta,
        safe_cast(motivo as string) as motivo,
        safe_cast(motivo_id as int64) as id_motivo,
        safe_cast(destino as string) as destino,
        safe_cast(destino_id as int64) as id_destino,
        safe_cast(tipo_destino as string) as tipo_destino,
        safe_cast(tipo_destino_id as int64) as id_tipo_destino,
        safe_cast(permanencia as int64) as permanencia,
        safe_cast(quantidade as int64) as quantidade,

        -- Metadados
        safe_cast(extracted_at as datetime) as extracted_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        date(data_particao) as data_particao

    from base64_para_string
    qualify row_number() over (partition by registro_id order by extracted_at desc) = 1
)

select * from renomeado
