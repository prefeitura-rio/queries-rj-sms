with pacientes as (
  select distinct
    -- id
    safe_cast(usuariocpf as int) as paciente_cpf,
    safe_cast(usuariocns as int) as paciente_cns,
    safe_cast(usuarionome as string) as paciente_nome,

    estabs.nome_acentuado as clinica_sf,
    estabs.area_programatica as clinica_sf_ap,
    estabs.telefone[SAFE_OFFSET(0)] as clinica_sf_telefone

  from {{ ref("raw_centralderegulacao_mysql__fibromialgia_relatorio") }}
  left join {{ ref("dim_estabelecimento") }} as estabs
  on safe_cast(unidaderefcnes as int) = safe_cast(id_cnes as int)
  where date(solicitacaodatahora) >= date '2024-01-01'
)

select * from pacientes
