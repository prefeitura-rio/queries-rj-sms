{{
    config(
        alias="funcionarios_ativos",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    source as (
        select
            cpf,
            nome,
            status_ativo,
            provimento_inicio,
            provimento_fim,
            data_vacancia,
            id_secretaria,
            secretaria_sigla,
            secretaria_nome,
            id_empresa,
            setor_nome,
            setor_sigla,
            setor_inicio,
            setor_fim,
            cargo_nome,
            cargo_categoria,
            cargo_subcategoria,
            empresa_nome,
            empresa_sigla,
            empresa_cnpj,
            cpf_particao
        from {{ source("brutos_ergon_staging", "funcionarios_ativos") }}
    )
select *
from source
