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
        '7015208' as cnes,
        'CMS Parque União' as name,
        '999999999' as phone
    ) as family_clinic,
    struct(
        '0000123456' as ine_code,
        'Equipe Fake II' as name,
        '999999999' as phone
    ) as family_health_team,

    [struct(
        'F0078A1101234567' as registry,
        'Francisco Cândido Xavier' as name
    )] as medical_responsible,
    [struct(
        'BEEF012345678900' as registry,
        'Pedro de Alcântara João Carlos Leopoldo' as name
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
        ['7015208'] as unidades_cadastro
    ) as exibicao,
    true as validated
