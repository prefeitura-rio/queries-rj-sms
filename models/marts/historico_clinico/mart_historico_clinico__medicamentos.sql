{{
    config(
        schema="saude_historico_clinico",
        alias="medicamentos-cronicos",
        materialized="table",
    )
}}


with 
    bruto_atendimento as (
        select * 
        from {{ ref('raw_prontuario_vitacare__atendimento') }} 
    ),
    prescricoes as (
        select 
            cpf,
            replace(json_extract_scalar(prescricoes_json, "$.cod_medicamento"), "-", "") as id,
            upper(json_extract_scalar(prescricoes_json, "$.nome_medicamento")) as nome,
            json_extract_scalar(prescricoes_json, "$.uso_continuado") as uso_continuo
        from bruto_atendimento,
            unnest(json_extract_array(prescricoes)) as prescricoes_json
    ),
    materiais as (
        select id_material, descricao, concentracao
        from {{ ref("dim_material") }}
    ),
    uso_continuado as (
        select 
            prescricoes.cpf,
            prescricoes.id,
            prescricoes.nome,
            coalesce(materiais.descricao, prescricoes.nome) as nome,
            materiais.concentracao,
        from prescricoes
            inner join materiais on prescricoes.id = materiais.id_material
        where prescricoes.uso_continuo = True
    )
select
    cpf,
    array_agg(
        struct(
            id,
            nome,
            concentracao
        )
    ) as medicamentos
from uso_continuado