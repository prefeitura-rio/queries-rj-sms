{{
    config(
        alias="exames"
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
)

SELECT 
    sha256(s.paciente_cpf) as paciente_id, 
    descricao_apoio as nome_exame,
    r.resultado,
    r.unidade,
    s.datahora_pedido
FROM  {{ref('raw_exames_laboratoriais__resultados')}} r
  inner join {{ref('raw_exames_laboratoriais__exames')}} e on r.id_exame = e.id
  inner join  {{ref('raw_exames_laboratoriais__solicitacoes')}} s on e.id_solicitacao = s.id
WHERE 
    s.paciente_cpf is not null and
    s.paciente_cpf in (select paciente_id from cadastros) and 
    descricao_apoio in (
        'GLICEMIA',
        'Hemoglobina A1',
        'INSULINA',
        'COLESTEROL TOTAL',
        'LDL',
        'HDL COLESTEROL',
        'TRIGLICERIDES',
        'CREATININA',
        'UREIA',
        'TGO',
        'GGT',
        'VHS',
        'T4L',
        'CALCULO DO HEMOGRAMA'
    )