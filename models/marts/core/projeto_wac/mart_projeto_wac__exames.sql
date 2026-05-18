{{
    config(
        alias="exames"
    )
}}

SELECT 
    sha256(s.paciente_cpf) as paciente_id, 
    descricao_apoio as nome_exame,
    r.resultado,
    r.unidade
FROM  {{ref('raw_exames_laboratoriais__resultados')}} r
  inner join {{ref('raw_exames_laboratoriais__exames')}} e on r.id_exame = e.id
  inner join  {{ref('raw_exames_laboratoriais__solicitacoes')}} s on e.id_solicitacao = s.id
WHERE 
    s.paciente_cpf is not null and 
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
        'RESULTADO TSH CONFIRMAÇÃO',
        'T4L',
        'CALCULO DO HEMOGRAMA'
    )