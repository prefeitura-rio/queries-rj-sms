{{
    config(
        alias="consultas"
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

atendimentos_vitai as (
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
),

boletins_vitai as (
  select
    SHA256(p.cpf) as paciente_id,
    upper(trim(b.atendimento_tipo)) as atendimento_tipo,
    cid_codigo,
    b.data_entrada as atendido_em
  from {{ ref('raw_prontuario_vitai__boletim') }} b
    inner join {{ ref('raw_prontuario_vitai__paciente') }} p on p.gid = b.gid_paciente
    inner join atendimentos_vitai a on b.gid = a.gid_boletim
  where
    p.cpf in (select paciente_id from cadastros)
    and p.cpf is not null 
    and trim(b.atendimento_tipo) in ('INTERNACAO','EMERGENCIA')
),

atendimentos_vitacare as (
    SELECT 
      sha256(a.patient_cpf) as paciente_id,
      upper(a.tipo_atendimento) as atendimento_tipo,
      c.cod_cid10 as cid_codigo,
      a.datahora_fim_atendimento as atendido_em
    FROM {{ ref('raw_prontuario_vitacare_historico__condicao') }} c
    INNER JOIN {{ ref('raw_prontuario_vitacare_historico__acto') }} a using (id_prontuario_global)
    WHERE 
        a.patient_cpf is not null AND
        a.patient_cpf in (select paciente_id from cadastros)
)

select * from boletins_vitai
union all
select * from atendimentos_vitacare
