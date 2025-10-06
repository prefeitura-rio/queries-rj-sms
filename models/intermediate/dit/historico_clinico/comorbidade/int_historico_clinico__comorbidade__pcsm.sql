{{
    config(
        schema="intermediario_historico_clinico",
        alias="comorbidades__pcsm",
        materialized="table",
    )
}}

with
    comorbidades as (
        select
            id_comorbidade,
            case
                when lower(descricao_comorbidade) in ('não relatou nenhuma')
                    then null
                when lower(descricao_comorbidade) in ('hiv','hpv')
                    then upper(descricao_comorbidade)
                when lower(descricao_comorbidade) in ('ave')
                    then 'Acidente Vascular Encefálico'
                when lower(descricao_comorbidade) in ('dpoc')
                    then 'Doença Pulmonar Obstrutiva Crônica'
                when lower(descricao_comorbidade) in ('f29.0', 'f29')
                    then 'Psicose não-orgânica não especificada'
                when lower(descricao_comorbidade) like '%f84.0%'
                    then 'Autismo infantil'
                when lower(descricao_comorbidade) like '%f20%'
                    then 'Esquizofrenia'
                when lower(descricao_comorbidade) like '%f20.5%'
                    then 'Esquizofrenia residual'
                when lower(descricao_comorbidade) like '%f21%'
                    then 'Transtorno esquizotípico'
                else 
                    initcap(trim(descricao_comorbidade))
            end as comorbidade,
            --codigo_doenca Na maior parte dos casos não é preenchido, vale a pena manter?
        from {{ref('raw_pcsm_comorbidades')}}
    ),

    comorbidades_paciente as (
        select 
            id_paciente,
            id_comorbidade
        from {{ref('raw_pcsm_comorbidades_pacientes')}}
    ),

    pacientes as (
        select 
            id_paciente,
            numero_cpf_paciente as cpf, 
            numero_cartao_saude as cns,
        from {{ref('raw_pcsm_pacientes')}}
    ),

    pacientes_comorbidades as (
        select 
            cp.id_paciente,
            c.comorbidade
        from comorbidades_paciente cp
        left join comorbidades c on cp.id_comorbidade = c.id_comorbidade
    )

select
    pc.id_paciente,
    p.cns, 
    p.cpf,
    pc.comorbidade
from pacientes_comorbidades pc
left join pacientes p on pc.id_paciente = p.id_paciente