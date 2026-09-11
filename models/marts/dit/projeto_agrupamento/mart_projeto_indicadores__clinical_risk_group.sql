{{
    config(
        alias="crg"
    )
}}

with

constantes as (
  select
    date('2024-09-01') as data_minima,
    date('2026-09-01') as data_maxima
),

estabelecimentos as (
    select
        id_cnes,
        nome_fantasia
    from {{ ref('dim_estabelecimento') }}
),

cadastros as (
    select
        cpf,
        nome,
        data_nascimento,
        sexo,
        data_atualizacao_cadastro
    from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
    qualify row_number() over (
        partition by cpf
        order by data_atualizacao_cadastro
    ) = 1
),


atendimentos as (
  select

    est.nome_fantasia as Hospital,
    a.id_cnes as Codigo_Hospital,
    a.patient_cpf as Codigo_Paciente,

    date(a.datahora_inicio_atendimento as Dt_Admissao,
    date(a.datahora_fim_atendimento) as Dt_Saida,

    c.sexo,
    
    -- Idade em Dias: caso paciente tenha menos de 1 ano, a idade será em dias, caso contrário, será em anos
    case 
        when date_diff(date(a.datahora_fim_atendimento), c.data_nascimento, year) < 1 
            then date_diff(date(a.datahora_fim_atendimento), c.data_nascimento, day)
        else null
    end as Idade_Dias,

    -- Idade em Anos
    case 
        when date_diff(date(a.datahora_fim_atendimento), c.data_nascimento, year) >= 1 
            then date_diff(date(a.datahora_fim_atendimento), c.data_nascimento, year)
        else null
    end as Idade_Anos,

    date_diff(date(a.datahora_fim_atendimento), c.data_nascimento, year) as Idade_Anos,

    null as Peso_ao_Nascer, -- Talvez seja possivel, mas dificil
    null as Status_Alta, -- Transformação, mas deve ficar entre 01 (alta normal) e 02 (transferido)

    null as CID_Principal, -- Como pegar ordenar por importancia?
    null as CID_Secundario, -- Ok

    'SUS' as Tipo_de_Tabela,

    null as Codigo_Internacao,
    null as DVM,
    null as Procedimento,

  from {{ ref('raw_prontuario_vitacare_historico__acto') }} a
    inner join cadastros c on c.cpf = a.patient_cpf
    inner join estabelecimentos est on a.id_cnes = est.id_cnes
    cross join constantes
  where
    a.tipo_consulta = 'Atendimento SOAP'
    and ap = '22'
    and date(a.datahora_fim_atendimento)
      between (select data_minima from constantes) and (select data_maxima from constantes)
)

select *
from atendimentos
