{{
    config(
        alias="equipe_saude_familia",
        materialized="table",
    )
}}

with

    quantitativo_atendimentos_vitacare as (
        select
            id_cadastro,
            count(*) as quantidade_atendimentos
        from {{ ref("raw_prontuario_vitacare_historico__acto") }}
        where datahora_fim_atendimento > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 YEAR)
        group by 1
    ),

    equipe_saude_familia_vitacare_cadastro as (
        select
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            id_cnes,
            ine_equipe as equipe_ine,
            equipe as equipe_nome,
            coalesce(q.quantidade_atendimentos, 0) as quantidade_atendimentos,

            greatest(updated_at, data_atualizacao_cadastro, data_atualizacao_vinculo_equipe) as updated_at
        from {{ ref("raw_prontuario_vitacare_historico__cadastro") }} c
            left join quantitativo_atendimentos_vitacare q on q.id_cadastro = c.id_global
        where cadastro_permanente = true and situacao_usuario = 'Ativo' and ine_equipe is not null
    )
select *
from equipe_saude_familia_vitacare_cadastro