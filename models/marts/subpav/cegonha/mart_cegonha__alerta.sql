{{ config(
    schema='projeto_cegonha',
    alias='alerta',
    materialized='incremental',
    tags=['cegonha_digital_15min']
) }}

with novas_interacoes as (

    select h.*
    from {{ ref('int_subpav_cegonha__alerta_historico') }} as h

    {% if is_incremental() %}
        where not exists (
            select 1
            from {{ this }} as a
            where a.id_interacao = h.id_interacao
        )
    {% endif %}

),

cpfs_alvo as (

    select distinct cpf_paciente
    from novas_interacoes
    where cpf_paciente is not null

),

historico_clinico as (

    select
        cpf,
        equipe_saude_familia[safe_offset(0)].nome as nome_equipe,
        equipe_saude_familia[safe_offset(0)].telefone as tel_equipe,
        equipe_saude_familia[safe_offset(0)].clinica_familia.nome as unidade_clinica_familia,
        equipe_saude_familia[safe_offset(0)].clinica_familia.telefone as tel_unidade_saude,
        endereco[safe_offset(0)].logradouro as logradouro,
        endereco[safe_offset(0)].numero as numero,
        endereco[safe_offset(0)].complemento as complemento,
        endereco[safe_offset(0)].cep as cep
    -- from `rj-sms.saude_historico_clinico.paciente`
    from {{ ref('mart_historico_clinico__paciente') }}
    where cpf in (select cpf_paciente from cpfs_alvo)
    qualify row_number() over (
        partition by cpf
        order by metadados.processed_at desc
    ) = 1

),

dados_maternidade as (

    select
        cpf,
        nome_maternidade_alta,
        cnes_maternidade_alta,
        data_parto,
        data_alta_internacao,
        desfecho_gestacao
    -- from `rj-sms.projeto_whatsapp.sisare_alta_maternidade`
    from {{ ref('mart_iplanrio__sisare_alta_maternidade') }}
    where cpf in (select cpf_paciente from cpfs_alvo)
    qualify row_number() over (
        partition by cpf
        order by data_alta_internacao desc
    ) = 1

),

final as (

    select
        n.id_interacao,
        n.tipo_alerta,
        n.ultima_resposta_usuario,
        n.penultima_resposta_usuario,
        n.Motivo as Motivo,
        n.Nome as Nome,
        n.Whatsapp as Whatsapp,
        n.data_nascimento,
        n.nome_paciente,
        n.nome_acompanhante,
        n.telefones_paciente,
        n.telefone_acompanhante,
        n.desfecho_gestacao,
        n.data_alta_internacao,
        n.data_parto,
        n.fim_datahora,
        n.cpf_paciente,
        n.id_contato,
        struct(
            hc.unidade_clinica_familia as unidade,
            hc.tel_unidade_saude as tel_unidade,
            hc.nome_equipe as nome,
            hc.tel_equipe as tel_equipe
        ) as equipe,
        struct(
            hc.logradouro as logradouro,
            hc.numero as numero,
            hc.complemento as complemento,
            hc.cep as cep
        ) as endereco,
        struct(
            m.nome_maternidade_alta as nome,
            m.cnes_maternidade_alta as cnes,
            m.data_parto as data_parto,
            m.data_alta_internacao as data_alta,
            m.desfecho_gestacao as desfecho
        ) as maternidade,
        'Não atendido' as status_atendimento,
        cast(null as string) as responsavel_atendimento
    from novas_interacoes as n
    left join historico_clinico as hc
        on n.cpf_paciente = hc.cpf
    left join dados_maternidade as m
        on n.cpf_paciente = m.cpf

)

select *
from final