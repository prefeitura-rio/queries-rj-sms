-- noqa: disable=LT08
-- to do: adicionar hci; adicionar receita federal; padronizar valores nas colunas; etc etc etc

{{
  config(
    enabled=true,
    schema="saude_dados_mestres",
    alias="paciente",
    unique_key='cpf',
    partition_by={
        "field": "cpf_particao",
        "data_type": "int64",
        "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
    on_schema_change='sync_all_columns',
    tags=["weekly"]
  )
}}

with
agg as (
  select
    paciente_cpf as cpf,
    cast(paciente_cpf as int) as cpf_particao,

    array_agg(distinct sistema_origem ignore nulls) as sistemas_origem,
    count(*) as n_registros,

    array_agg(distinct paciente_cns ignore nulls) as cns_lista,

    array_agg(distinct paciente_nome ignore nulls) as nomes,
    array_agg(distinct paciente_nome_mae ignore nulls) as nomes_mae,
    array_agg(distinct paciente_nome_pai ignore nulls) as nomes_pai,

    array_agg(distinct cast(paciente_data_nascimento as string) ignore nulls) as datas_nascimento,
    array_agg(distinct cast(paciente_idade as string) ignore nulls) as idades,
    array_agg(distinct paciente_sexo ignore nulls) as sexos,
    array_agg(distinct paciente_racacor ignore nulls) as racas_cores,
    array_agg(distinct paciente_etnia ignore nulls) as etnias,

    array_agg(distinct paciente_uf_nascimento ignore nulls) as ufs_nascimento,
    array_agg(distinct paciente_municipio_nascimento ignore nulls) as municipios_nascimento,

    array_agg(distinct paciente_uf_residencia ignore nulls) as ufs_residencia,
    array_agg(distinct paciente_municipio_residencia ignore nulls) as municipios_residencia,
    array_agg(distinct paciente_bairro_residencia ignore nulls) as bairros_residencia,
    array_agg(distinct paciente_cep_residencia ignore nulls) as ceps_residencia,
    array_agg(distinct paciente_endereco_residencia ignore nulls) as enderecos_residencia,
    array_agg(distinct paciente_complemento_residencia ignore nulls) as complementos_residencia,
    array_agg(distinct paciente_numero_residencia ignore nulls) as numeros_residencia,
    array_agg(distinct paciente_tp_logradouro_residencia ignore nulls) as tipos_logradouro_residencia,

    array_agg(distinct paciente_mun_origem ignore nulls) as municipios_origem_sih,
    array_agg(distinct id_paciente_municipio_ibge ignore nulls) as ids_municipio_ibge,

    array_agg(distinct paciente_tipo_logradouro ignore nulls) as tipos_logradouro_sih,
    array_agg(distinct paciente_logradouro ignore nulls) as logradouros_sih,

    -- checagens de conflitos
    (select count(distinct safe_cast(paciente_data_nascimento as string)) from {{ref("int_dim_paciente__esquema_canonico")}} x where x.paciente_cpf = agg_src.paciente_cpf) > 1
      as data_nascimento_conflitantes,

    (select count(distinct paciente_sexo) from {{ref("int_dim_paciente__esquema_canonico")}} x where x.paciente_cpf = agg_src.paciente_cpf and paciente_sexo is not null) > 1
      as sexos_conflitantes,

    (select count(distinct paciente_nome_mae) from {{ref("int_dim_paciente__esquema_canonico")}} x where x.paciente_cpf = agg_src.paciente_cpf and paciente_nome_mae is not null) > 1
      as nome_mae_conflitantes

  from {{ref("int_dim_paciente__esquema_canonico")}} as agg_src
  where paciente_cpf is not null
  group by paciente_cpf, cpf_particao
)

select
  cpf,
  cpf_particao,
  sistemas_origem,
  n_registros,
  cns_lista,

  nomes,
  sexos,
  racas_cores,
  etnias,
  datas_nascimento,
  idades,
  nomes_mae,
  nomes_pai,

  ufs_nascimento,
  municipios_nascimento,

  ufs_residencia,
  municipios_residencia,
  bairros_residencia,
  ceps_residencia,
  enderecos_residencia,
  complementos_residencia,
  numeros_residencia,
  tipos_logradouro_residencia,

  municipios_origem_sih,
  ids_municipio_ibge,
  tipos_logradouro_sih,
  logradouros_sih,

  data_nascimento_conflitantes,
  sexos_conflitantes,
  nome_mae_conflitantes
from agg
