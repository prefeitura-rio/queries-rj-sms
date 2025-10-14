{{
    config(
        alias="exame_imagem",
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
    '2277271' as unidade_cnes,
    'INST DE MEDICINA VETERINARIA JORGE VAITSMAN' as unidade_nome,

    '42298037299' as paciente_cpf,

    'TC DE SELA TURCICA' as exame_nome,
    '0206010060' as exame_codigo_sigtap,

    '000001-001' as id_exame,
    '1' as id_laudo,
    'gs://falta-mock-pdf' as laudo_bucket,  -- FIXME

    cast('2025-10-01' as DATE) as exame_data,
    cast('2025-10-05T12:34:56.0000' as DATETIME) as laudo_data_atualizacao,

    'Pedro M.' as medico_requisitante,
    'Daniel S.' as medico_responsavel,
    'Rodrigo P.' as medico_revisor,

    42298037299 as cpf_particao
