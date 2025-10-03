{{
    config(
        alias="paciente",
        schema="app_historico_clinico_treinamento",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

select 
    42298037299 as cpf_particao,
    '42298037299' as cpf,
    'Paciente Fake I' as registration_name,
    null as social_name,
    '700000000000000' as cns,
    '1989-01-01' as birth_date,
    'masculino' as gender,
    'parda' as race,
    false as deceased,
    '912345678' as phone,
    struct(
        '6664040' as cnes,
        'CF Heitor dos Prazeres' as name,
        '994237148' as phone
    ) as family_clinic,
    struct(
        '0000307831' as ine_code,
        'Boa Viagem' as name,
        '991396543' as phone
    ) as family_health_team,

    [struct(
        '612769D381064B82' as registry,
        'Tuany de Paula Ferreira' as name
    )] as medical_responsible,
    [struct(
        '9C04A670661146E2' as registry,
        'Naya Bernasconi Nunes Avenia Puertas' as name
    )] as nursing_responsible,

    struct(
        '01234' as id_pcsm,
        'Ativo' as status_acompanhamento,
        'CAPS Neusa Santos Souza' as nome_unidade,
        '7926103' as cnes,
        ['2136138285', '21970114479'] as telefones
    ) as mental_health,

    struct(
        true as indicador,
        [] as motivos,
        ['31'] as ap_cadastro,
        ['6664040'] as unidades_cadastro
    ) as exibicao,
    true as validated
