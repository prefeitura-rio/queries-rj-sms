{{
    config(
        alias="pacientes_invalidos",
        materialized="table",
        schema="intermediario_historico_clinico",
    )
}}

with dados as (
    select cpf,
        d.nome,
        d.data_nascimento 
    from {{ ref('int_historico_clinico__paciente__vitai') }}, unnest(dados) as d
    where not(lower(d.nome) like '%teste%')
    and array_length(split(d.nome, ' '))>2
    union all
    select cpf,
        d.nome,
        d.data_nascimento 
    from {{ ref('int_historico_clinico__paciente__vitacare') }}, unnest(dados) as d
    where not(lower(d.nome) like '%teste%')
    and array_length(split(d.nome, ' '))>2
    union all
    select cpf,
        d.nome, 
        d.data_nascimento 
    from {{ ref('int_historico_clinico__paciente__smsrio') }}, unnest(dados) as d
    where not(lower(d.nome) like '%teste%')
    and array_length(split(d.nome, ' '))>2
),
cpfs_unicos as (
select distinct dados_cpf_unico.cpf,
    dados.nome, 
    dados.data_nascimento
from (select distinct cpf from dados) as dados_cpf_unico
left join dados 
on dados.cpf = dados_cpf_unico.cpf
),
cpfs_distancias as (
select 
  cpfs_unicos.cpf, 
  edit_distance(cpfs_unicos.nome, dados.nome)/if(char_length(least(cpfs_unicos.nome, dados.nome))=0,
                                                1,
                                                char_length(least(cpfs_unicos.nome, dados.nome))) as lev_nome,
  edit_distance(regexp_extract(cpfs_unicos.nome, '([^ ]*) '),regexp_extract(dados.nome, '([^ ]*) ')) as lev_primeiro_nome,
  cpfs_unicos.nome as nome_cadastro_1,
  dados.nome as nome_cadastro_2,
  regexp_extract(cpfs_unicos.nome, '([^ ]*) ') as lev_primeiro_nome_cadasto_1,
  regexp_extract(dados.nome, '([^ ]*) ') as lev_primeiro_nome_cadasto_2,
  edit_distance(cast(cpfs_unicos.data_nascimento as string), cast(dados.data_nascimento as string)) as lev_nascimento,
  cpfs_unicos.data_nascimento as data_nascimento_cadastro_1,
  dados.data_nascimento as data_nascimento_cadastro_2
from cpfs_unicos 
inner join dados 
on dados.cpf = cpfs_unicos.cpf
)
select * 
from cpfs_distancias
where (lev_primeiro_nome > 1)
-- ((lev_nome > 0.3 and lev_nascimento > 2) 
-- or (lev_nome > 0.5)
