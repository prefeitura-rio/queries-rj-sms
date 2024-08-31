{{
    config(
        schema="saude_historico_clinico",
        alias="medicamento_cronico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with
    atendimentos_recentes as (
        select *
        from {{ ref("raw_prontuario_vitacare__atendimento") }}
        where updated_at >= date_sub(current_date(), interval 1 year)
    ),
    prescricoes as (
        select
            cpf,
            replace(
                json_extract_scalar(prescricoes_json, "$.cod_medicamento"), "-", ""
            ) as id,
            json_extract_scalar(prescricoes_json, "$.nome_medicamento") as nome,
            cast(
                json_extract_scalar(prescricoes_json, "$.uso_continuado") as boolean
            ) as uso_continuo,
            updated_at as datahora_prescricao
        from
            atendimentos_recentes,
            unnest(json_extract_array(prescricoes)) as prescricoes_json
    ),
    materiais as (
        select id_material, descricao, concentracao from {{ ref("dim_material") }}
    ),
    uso_continuado as (
        select
            prescricoes.cpf,
            prescricoes.id,
            {{ proper_br("coalesce(materiais.descricao, prescricoes.nome)") }} as nome,
            materiais.concentracao,
            prescricoes.datahora_prescricao,
            safe_cast(prescricoes.cpf as int64) as cpf_particao
        from prescricoes
        inner join materiais on prescricoes.id = materiais.id_material
        where prescricoes.uso_continuo = true
    ),

    final as (
        select
            cpf as paciente_cpf,
            cpf_particao,
            array_agg(
                struct(id, nome, concentracao, datahora_prescricao)
            ) as medicamentos,
            struct(current_timestamp() as processed_at) as metadados
        from uso_continuado
        group by cpf, cpf_particao
    )

select paciente_cpf, medicamentos, metadados, cpf_particao from final