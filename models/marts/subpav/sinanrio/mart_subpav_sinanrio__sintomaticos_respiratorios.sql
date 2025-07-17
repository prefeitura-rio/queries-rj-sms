{{
    config(
        materialized='view',
        alias = "subpav_sinanrio__sintomaticos_respiratorios",
        schema = "projeto_subpav",
    )
}}

with atendimentos_com_cids as (

    select
        a.*,
        array_concat(
            ARRAY(
                SELECT cod
                FROM UNNEST(JSON_EXTRACT_ARRAY(a.condicoes)) AS item,
                    UNNEST([
                        JSON_VALUE(item, '$.cod_cid10'),
                        JSON_VALUE(item, '$.cod_ciap2')
                    ]) AS cod
                WHERE cod IS NOT NULL AND cod != ''
                ),
            regexp_extract_all(upper(a.soap_subjetivo_motivo), r'\b[A-Z][0-9]{3}\b'),
            regexp_extract_all(upper(a.soap_objetivo_descricao), r'\b[A-Z][0-9]{3}\b'),
            regexp_extract_all(upper(a.soap_avaliacao_observacoes), r'\b[A-Z][0-9]{3}\b')
        ) as cids_extraidos

    from `brutos_prontuario_vitacare.atendimento` a
    left join `brutos_prontuario_vitacare.paciente` p
        on a.cpf = p.cpf
    where date(a.datahora_inicio) >= date_sub(current_date(), interval 1 day)

),

-- Filtra atendimentos que contêm ao menos 1 CID da lista macro
atendimentos_filtrados as (
    select *
    from atendimentos_com_cids
    where exists (
        select cid
        from unnest(cids_extraidos) as cid
        where cid in UNNEST({{ sinanrio_lista_cids_sintomaticos() }})
    )
),

-- Retorna apenas o atendimento mais recente por CPF
atendimento_unico as (
    select *,
        row_number() over (partition by cpf order by datahora_inicio desc) as rn
    from atendimentos_filtrados
)

select
    *
from atendimento_unico
where rn = 1
