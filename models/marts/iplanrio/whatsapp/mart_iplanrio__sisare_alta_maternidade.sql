{{ config(
    schema = 'projeto_whatsapp',
    alias = 'sisare_alta_maternidade',
    materialized = 'table',
    cluster_by = ['cpf', 'cnes_maternidade_alta']
) }}

with gestantes as (

    select
        cast(id_gestante as string) as id_gestante,
        cast(id_paciente as string) as id_paciente,
        cast(id_internacao as string) as id_internacao,
        cpf,
        nome,
        telefone as telefone_informado,
        dt_parto as data_fim_gestacao,
        id_desfecho_internacao,
        id_desfecho_gestacao
    from {{ ref('int_subpav__sisare_gestantes') }}
    where id_gestante is not null
      and id_paciente is not null
      and id_internacao is not null
      and id_desfecho_internacao in (1, 3)

),

internacoes as (

    select
        cast(id_internacao as string) as id_internacao,
        dt_saida as data_alta_internacao,
        cast(unidade_atendimento as string) as cnes_maternidade_alta
    from {{ ref('raw_plataforma_subpav_sisare__internacoes') }}
    where id_internacao is not null
    and dt_saida >= date_trunc(current_date('America/Sao_Paulo'), year)

),

desfechos_gestacao as (

    select
        id_desfecho_gestacao,
        descricao as desfecho_gestacao
    from {{ ref('raw_plataforma_subpav_sisare__desfechos_gestacao') }}

),

estabelecimento as (

    select
        cast(id_cnes as string) as cnes_maternidade_alta,
        any_value(nome_fantasia) as nome_maternidade_alta
    from {{ ref('raw_gdb_cnes__estabelecimento') }}
    where id_cnes is not null
    group by 1

),

base as (

    select
        g.cpf as cpf,
        g.nome as nome,
        g.telefone_informado,
        {{ padroniza_telefone_whatsapp('g.telefone_informado') }} as tel,
        i.data_alta_internacao,
        regexp_replace(i.cnes_maternidade_alta, r'\D', '') as cnes_maternidade_alta,
        e.nome_maternidade_alta,
        g.data_fim_gestacao,
        g.id_desfecho_gestacao,
        d.desfecho_gestacao
    from gestantes g
    inner join internacoes i
        on i.id_internacao = g.id_internacao
    left join desfechos_gestacao d
        on d.id_desfecho_gestacao = g.id_desfecho_gestacao
    left join estabelecimento e
        on e.cnes_maternidade_alta = regexp_replace(i.cnes_maternidade_alta, r'\D', '')

)

select
    cpf,
    nome,
    telefone_informado,
    tel.telefone_valido_whatsapp,
    tel.motivo_invalidacao_telefone,
    data_alta_internacao,
    cnes_maternidade_alta,
    nome_maternidade_alta,
    data_fim_gestacao,
    id_desfecho_gestacao,
    desfecho_gestacao
from base