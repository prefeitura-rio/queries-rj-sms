{{
    config(
        alias="cids"
    )
}}

with

cadastros as (
  select
    cpf as paciente_id,
  from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
  where
    id_cnes = '2280787'
    and cpf is not NULL
    and ine_equipe is not NULL
),

condicoes_atribuidas as (
    SELECT 
        a.patient_cpf as paciente_id, 
        c.cod_cid10, 
        cast(data_diagnostico as date) as data_diagnostico
    FROM {{ ref('raw_prontuario_vitacare_historico__condicao') }} c
    INNER JOIN {{ ref('raw_prontuario_vitacare_historico__acto') }} a using (id_prontuario_global)
    WHERE 
        a.patient_cpf is not null AND
        a.patient_cpf in (select patient_id from cadastros)
),

condicoes_do_paciente as (
    select *
    from condicoes_atribuidas
    qualify row_number() over (
        partition by paciente_id, cod_cid10
        order by data_diagnostico desc
    ) = 1
)

select
    sha256(paciente_id) as paciente_id,
    cod_cid10 as cid,
    data_diagnostico
from condicoes_do_paciente