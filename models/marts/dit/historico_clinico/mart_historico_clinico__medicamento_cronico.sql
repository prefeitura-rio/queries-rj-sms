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
        and cpf is not null
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
        order by cpf, nome, datahora_prescricao desc
    ),

    prescricoes_mais_recentes as (
        select * from prescricoes
        qualify row_number() over (partition by cpf, id order by datahora_prescricao desc) = 1
    ),

    materiais as (
        select id_material, descricao, concentracao from {{ ref("dim_material") }}
    ),

    uso_continuado as (
        select
            prescricoes.cpf,
            prescricoes.id,
            {{ proper_br("coalesce(materiais.descricao, upper(prescricoes.nome))") }} as nome,
            materiais.concentracao,
            prescricoes.datahora_prescricao,
            safe_cast(prescricoes.cpf as int64) as cpf_particao
        from prescricoes_mais_recentes as prescricoes
        inner join materiais on prescricoes.id = materiais.id_material
        where prescricoes.uso_continuo = true
        order by prescricoes.cpf, nome
    ),

    uso_continuado_somente_concentracao_mais_recente as (
        select * from uso_continuado
        qualify row_number() over (partition by cpf, nome order by datahora_prescricao desc) = 1
        order by cpf, nome
    ),

    final as (
        select
            cpf as paciente_cpf,
            cpf_particao,
            array_agg(
                struct(id, nome, concentracao, datahora_prescricao)
            ) as medicamentos,
            struct(current_timestamp() as processed_at) as metadados
        from uso_continuado_somente_concentracao_mais_recente
        group by cpf, cpf_particao
    )

select paciente_cpf, medicamentos, metadados, cpf_particao from final

