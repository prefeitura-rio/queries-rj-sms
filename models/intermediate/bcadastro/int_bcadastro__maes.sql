{{
    config(
        alias="maes",
        materialized="table",
        schema="intermediario_bcadastro",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

-- Primeiro selecionamos nomes candidatos para mães
-- Só usamos nomes com poucas ocorrências, senão não teremos
--  como saber qual CPF é o correto posteriormente
with nomes_quase_unicos as (
    select
        tb.nome,
        count(*) as qtd
    from {{ ref("int_bcadastro__nome") }} as tb
    group by 1
    -- Máximo de mães em potencial com o exato mesmo nome
    -- antes de considerarmos difícil demais fazer a separação:
    having qtd <= 3
),

-- A partir dos nomes, preechemos com CPF e hash
nome_para_cpf as (
    select
        tb.cpf,
        tb.nome,
        tb.hash_particao
    from nomes_quase_unicos
    -- FIXME: join pesado :\
    left join {{ ref("int_bcadastro__nome") }} as tb
        on nomes_quase_unicos.nome = tb.nome
),

-- Todos os pacientes
source as (
    select
        tb.cpf,
        tb.mae_nome,
        farm_fingerprint(
            split(tb.mae_nome, " ")[safe_offset(0)]
        ) as mae_hash
    from {{ ref("raw_bcadastro__cpf") }} as tb
    tablesample system (1 percent) -- FIXME
    -- Ignorando sem nome de mãe
    where tb.mae_nome is not null
),

-- 
joined as (
    select
        source.cpf,
        nome_para_cpf.cpf as mae_cpf_candidato
    from nome_para_cpf
    left join source
        on (
            source.mae_hash = nome_para_cpf.hash_particao
            and source.mae_nome = nome_para_cpf.nome
        )
    -- group by aqui estoura a memória ;x
)

select
    cpf,
    mae_cpf_candidato,
    safe_cast(cpf as int64) as cpf_particao
from joined
where mae_cpf_candidato is not null
