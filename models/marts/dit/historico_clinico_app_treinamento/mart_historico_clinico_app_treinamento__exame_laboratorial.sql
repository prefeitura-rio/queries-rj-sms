{{
    config(
        alias="exame_laboratorial",
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
    '42298037299' as paciente_cpf,

    [struct(
        "Experimento de Íons Grav." as descricao,
        "GBAR" as codigo,
        cast("2025-03-14T18:04:55" as datetime) as datahora_assinatura
    )] as exames,

    "Pedro Marques" as medico_solicitante,
    "Inst de Medicina Veterinária Jorge Vaitsman" as unidade_nome,

    "https://storage.googleapis.com/sms_dit_arquivos_publicos/hci/mock-cientificalab.pdf" as laudo_url,
    cast("2025-01-04T16:43:00" as datetime) as datahora_pedido,
    cast("2025-03-14T18:04:55" as datetime) as _ultima_datahora_assinatura
