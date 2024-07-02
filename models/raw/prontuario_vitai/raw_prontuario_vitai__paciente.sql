{{ config(
    materialized='incremental',
    unique_key='cpf'
) }}

with recent_events as (
    select
        *,
        row_number() over (partition by cpf order by updated_at desc) as row_num
    from
        {{ ref("raw_prontuario_vitai__paciente_eventos_recentes") }}
), 
latest_events as (
    select
        *
    from
        recent_events
    where
        row_num = 1
)

-- Se a tabela 'paciente' não existir, crie-a
select
    *
from
    latest_events

-- Se a tabela 'paciente' já existir, faça o merge
{% if is_incremental() %}
merge into {{ this }} as target
using latest_events as source
    on target.cpf = source.cpf
when matched and source.datalake_imported_at > target.datalake_imported_at then
    update set
        t.raca_cor = s.raca_cor,
        t.nome_alternativo = s.nome_alternativo,
        t.estabelecimento_gid = s.estabelecimento_gid,
        t.complemento = s.complemento,
        t.cns = s.cns,
        t.data_nascimento = s.data_nascimento,
        t.trans_genero = s.trans_genero,
        t.tipo_logradouro = s.tipo_logradouro,
        t.nome_logradouro = s.nome_logradouro,
        t.uf = s.uf,
        t.nacionalidade = s.nacionalidade,
        t.ocupacao_cbo = s.ocupacao_cbo,
        t.municipio = s.municipio,
        t.gid = s.gid,
        t.telefone = s.telefone,
        t.nome_mae = s.nome_mae,
        t.sexo = s.sexo,
        t.naturalidade = s.naturalidade,
        t.data_hora = s.data_hora,
        t.pais_nascimento = s.pais_nascimento,
        t.bairro = s.bairro,
        t.data_obito = s.data_obito,
        t.numero = s.numero,
        t.id_cidadao = s.id_cidadao,
        t.numero_prontuario = s.numero_prontuario,
        t.nome = s.nome,
        t.cliente = s.cliente,
when not matched then
    insert (raca_cor, nome_alternativo, estabelecimento_gid, complemento, cns, data_nascimento, trans_genero, tipo_logradouro, nome_logradouro, uf, nacionalidade, ocupacao_cbo, municipio, gid, telefone, nome_mae, sexo, naturalidade, data_hora, pais_nascimento, bairro, data_obito, numero, id_cidadao, cpf, numero_prontuario, nome, cliente, datalake_imported_at)
    values (s.raca_cor, s.nome_alternativo, s.estabelecimento_gid, s.complemento, s.cns, s.data_nascimento, s.trans_genero, s.tipo_logradouro, s.nome_logradouro, s.uf, s.nacionalidade, s.ocupacao_cbo, s.municipio, s.gid, s.telefone, s.nome_mae, s.sexo, s.naturalidade, s.data_hora, s.pais_nascimento, s.bairro, s.data_obito, s.numero, s.id_cidadao, s.cpf, s.numero_prontuario, s.nome, s.cliente, s.datalake_imported_at);
{% endif %}
