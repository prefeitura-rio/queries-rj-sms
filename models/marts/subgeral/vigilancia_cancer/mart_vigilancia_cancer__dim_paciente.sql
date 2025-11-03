-- noqa: disable=LT08

{{
  config(
    enabled=true,
    schema="projeto_vigilancia_cancer",
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
    cpf_particao,

    array_agg(distinct sistema_origem ignore nulls) as sistemas_origem,
    count(*) as n_registros,

    array_agg(distinct paciente_cns ignore nulls) as cns_lista,

    array_agg(distinct paciente_nome ignore nulls) as nomes,
    array_agg(distinct paciente_nome_mae ignore nulls) as nomes_mae,
    array_agg(distinct paciente_nome_pai ignore nulls) as nomes_pai,
    array_agg(distinct paciente_nome_social ignore nulls) as nomes_sociais,

    array_agg(distinct cast(paciente_data_nascimento as string) ignore nulls) as datas_nascimento,
    array_agg(distinct paciente_sexo ignore nulls) as sexos,
    array_agg(distinct paciente_racacor ignore nulls) as racas_cores,

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

    -- checagens de conflitos
    (select count(distinct safe_cast(paciente_data_nascimento as string)) from {{ref("int_dim_paciente__esquema_canonico")}} x where x.cpf_particao = agg_src.cpf_particao) > 1
      as data_nascimento_conflitantes,

    (select count(distinct paciente_sexo) from {{ref("int_dim_paciente__esquema_canonico")}} x where x.cpf_particao = agg_src.cpf_particao and paciente_sexo is not null) > 1
      as sexos_conflitantes,

    (select count(distinct paciente_nome_mae) from {{ref("int_dim_paciente__esquema_canonico")}} x where x.cpf_particao = agg_src.cpf_particao and paciente_nome_mae is not null) > 1
      as nome_mae_conflitantes

  from {{ref("int_dim_paciente__esquema_canonico")}} as agg_src
  where paciente_cpf is not null
  group by cpf_particao
),

final as (
  select
    lpad(safe_cast(cpf_particao as string), 11) as cpf,
    cpf_particao,
    sistemas_origem,
    n_registros,
    cns_lista,

    nomes,
    datas_nascimento,
    nomes_mae,
    nomes_pai,

    sexos,
    racas_cores,

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

    data_nascimento_conflitantes,
    sexos_conflitantes,
    nome_mae_conflitantes
  from agg
)

select * from final