{{
    config(
        alias="urgencia_emergencia"
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

atendimentos as (
  select 
    gid_boletim,
    cid_codigo,
    cid_nome,
    updated_at
  from {{ ref('raw_prontuario_vitai__atendimento') }}
  qualify row_number() over (
    partition by gid_boletim
    order by updated_at desc
  ) = 1
)

select
  SHA256(p.cpf) as paciente_id,
  trim(b.atendimento_tipo) as atendimento_tipo,
  cid_codigo,
  cid_nome,
  b.data_entrada as atendido_em
from {{ ref('raw_prontuario_vitai__boletim') }} b
  inner join {{ ref('raw_prontuario_vitai__paciente') }} p on p.gid = b.gid_paciente
  inner join atendimentos a on b.gid = a.gid_boletim
where
  p.cpf in (select paciente_id from cadastros)
  and p.cpf is not null 
  and trim(b.atendimento_tipo) in ('INTERNACAO','EMERGENCIA')
