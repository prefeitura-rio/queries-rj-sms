{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_vinculo_trabalho_pacientes",
        materialized="table",
        tags=["raw", "pcsm"],
        description="Tabela associativa que contem a lista de tipos de vínculos de trabalho de cada paciente. Esta tabela não existe no sistema PCSM, mas foi criada para facilitar a análise dos vínculos de trabalho dos pacientes."
    )
}} 

with 
    vinculo_trabalho_paciente as 
    (
    select id_paciente, b as vinculo_trabalho 
    from {{ source('brutos_prontuario_carioca_saude_mental', 'pacientes') }},
    unnest(split(vinculo_trabalho_paciente, ',')) as b
    )

select a.id_paciente, a.vinculo_trabalho as codigo_vinculo_trabalho
from vinculo_trabalho_paciente a
where trim(ifnull(a.vinculo_trabalho, '')) <> ''