{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="beneficios_pacientes",
        materialized="table",
        tags=["raw", "pcsm"],
        description="Tabela associativa que contem a lista de beneficios sociais recebidos por cada paciente. Esta tabela não existe no sistema PCSM, mas foi criada para facilitar a análise dos beneficios associados aos pacientes."
    )
}} 

with 
    beneficios_paciente as 
    (
    select id_paciente, b as beneficio 
    from {{ source('brutos_prontuario_carioca_saude_mental', 'pacientes') }},
    unnest(split(qual_beneficio, ',')) as b
    )

select a.id_paciente, a.beneficio as codigo_beneficio, current_timestamp() as transformed_at
from beneficios_paciente a
where trim(ifnull(a.beneficio, '')) <> ''