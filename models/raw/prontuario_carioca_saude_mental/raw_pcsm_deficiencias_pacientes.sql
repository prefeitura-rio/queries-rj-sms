{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="deficiencias_pacientes",
        materialized="table",
        tags=["raw", "pcsm"],
        description="Tabela associativa que contem a lista de deficiências de cada paciente. Esta tabela não existe no sistema PCSM, mas foi criada para facilitar a análise das deficiências dos pacientes."
    )
}} 

with 
    deficiencias_paciente as 
    (
    select id_paciente, b as deficiencia 
    from {{ source('brutos_prontuario_carioca_saude_mental', 'pacientes') }},
    unnest(split(tipo_deficiencia, ',')) as b
    )

select a.id_paciente, a.deficiencia as codigo_deficiencia, current_timestamp() as transformed_at
from deficiencias_paciente a
where trim(ifnull(a.deficiencia, '')) not in ('', 'N')