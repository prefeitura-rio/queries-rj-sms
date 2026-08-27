{{
    config(
        alias="atendimento_domiciliar_paciente",
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
    from {{ source("brutos_prontuario_sarah_padi_staging", "atendimento_domiciliar_pacientes") }}
    {% if is_incremental() %}
        where date(data_particao) >= date( '{{ last_partition }}' )
    {% endif %}
),

base64_para_string as (
    select
        {{ base64_to_string('atendimento_id') }} as atendimento_id,
        {{ base64_to_string('atendimento_registro') }} as atendimento_registro,
        {{ base64_to_string('atendimento_admissao') }} as atendimento_admissao,
        {{ base64_to_string('atendimento_alta') }} as atendimento_alta,
        {{ base64_to_string('alta_no_periodo') }} as alta_no_periodo,
        {{ base64_to_string('admitido_no_periodo') }} as admitido_no_periodo,
        {{ base64_to_string('unidade') }} as unidade,
        {{ base64_to_string('unidade_id') }} as unidade_id,
        {{ base64_to_string('data_transferencia') }} as data_transferencia,
        {{ base64_to_string('unidade_origem') }} as unidade_origem,
        {{ base64_to_string('servico') }} as servico,
        {{ base64_to_string('servico_id') }} as servico_id,
        {{ base64_to_string('rota') }} as rota,
        {{ base64_to_string('rota_id') }} as rota_id,
        {{ base64_to_string('bairro') }} as bairro,
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
        {{ base64_to_string('tipo_origem') }} as tipo_origem,
        {{ base64_to_string('tipo_origem_id') }} as tipo_origem_id,
        {{ base64_to_string('cuidador') }} as cuidador,
        {{ base64_to_string('nome_cuidador') }} as nome_cuidador,
        {{ base64_to_string('telefone_cuidador') }} as telefone_cuidador,
        {{ base64_to_string('parentesco_cuidador') }} as parentesco_cuidador,
        {{ base64_to_string('cids') }} as cids,
        {{ base64_to_string('avd') }} as avd,
        {{ base64_to_string('avd_no_periodo') }} as avd_no_periodo,
        {{ base64_to_string('avd_data') }} as avd_data,
        {{ base64_to_string('avd_dias') }} as avd_dias,
        {{ base64_to_string('avd_elegivel') }} as avd_elegivel,
        {{ base64_to_string('avd_modalidade') }} as avd_modalidade,
        {{ base64_to_string('avd_motivo') }} as avd_motivo,
        {{ base64_to_string('avd_motivo_detalhe') }} as avd_motivo_detalhe,
        {{ base64_to_string('avd_prof') }} as avd_prof,
        {{ base64_to_string('avd_cargo') }} as avd_cargo,
        {{ base64_to_string('demanda_judicial') }} as demanda_judicial,
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
        {{ base64_to_string('tempo') }} as tempo,
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
        atendimento_id as id_atendimento,
        date(atendimento_registro) as registro_data,
        date(atendimento_admissao) as admissao_data,
        date(atendimento_alta) as alta_data,
        alta_no_periodo as alta_periodo,
        admitido_no_periodo as admitido_periodo,

        unidade as unidade_nome,
        unidade_id as unidade_id,
        date(data_transferencia) as transferencia_data,
        unidade_origem as unidade_origem_nome,
        
        servico_id,
        servico as servico_tipo, 

        rota_id,
        rota,

        -- Dados do Paciente
        paciente_id,
        paciente_cpf,
        paciente_nome,
        paciente_sexo,
        paciente_raca_cor,
        paciente_genero_id,
        paciente_genero,
        date(paciente_dtnasc) as paciente_nascimento_data,
        paciente_idade,
        faixa_etaria_id,
        faixa_etaria, 
        cids,
        municipio_uf as paciente_endereco_municipio_uf,
        bairro as paciente_endereco_bairro,
        endereco as paciente_endereco,
        contato as contato,
        origem_id,
        origem, 
        tipo_origem_id as id_tipo_origem,
        tipo_origem as origem_tipo,
        
        -- Dados do Cuidador
        cuidador as cuidador,
        nome_cuidador as cuidador_nome,
        telefone_cuidador as cuidador_telefone,
        parentesco_cuidador as cuidador_parentesco,

        -- Atividades de Vida Diária
        avd,
        avd_no_periodo as avd_periodo,
        date(avd_data) as avd_data,
        avd_dias,
        avd_elegivel,
        avd_modalidade,
        avd_motivo,
        avd_motivo_detalhe,
        avd_prof as avd_profissional_nome,
        avd_cargo as avd_profissional_cargo,

        -- Informações de cuidado do paciente
        demanda_judicial,
        respiracao,
        interface,
        modo_ventilatorio,
        dispositivo_invasivo,
        spict_br,
        iaec_ad,
        linha_cuidado,
        cuidado_paliativo,
        cast(barthel_inicial as int64) as indice_barthel_inicial,
        cast(barthel_final as int64) as indice_barthel_final,
        cast(tempo as int64) as tempo,
        cast(permanencia as int64) as permanencia,
        cast(quantidade as int64) as quantidade,

        -- Metadados
        datetime(extracted_at) as extracted_at,
        current_datetime('America/Sao_Paulo') as processed_at,
        cast(ano_particao as int64) as ano_particao,
        cast(mes_particao as int64) as mes_particao,
        date(data_particao) as data_particao
    from base64_para_string
    qualify row_number() over(partition by id_atendimento order by extracted_at desc) = 1
)

select * from renomeado
