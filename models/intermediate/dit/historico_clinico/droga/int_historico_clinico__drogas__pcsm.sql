{{
    config(
        schema="intermediario_historico_clinico",
        alias="saude_mental_drogas_pcsm",
        materialized="table",
    )
}}

with
    drogas as (
        select 
            id_droga,
            case
                when lower(descricao_droga) in ('alcool', 'álcool (bebidas)', 'bebidas álcool')
                    then 'Álcool'
                when lower(descricao_droga) in ('lólo.', 'cheirinho da loló')
                    then 'Lólo'
                when lower(descricao_droga) in ('não soube dizer', 'não ha')
                    then null
                when lower(descricao_droga) in ('lsd', 'mdma')
                    then upper(descricao_droga)
                when lower(descricao_droga) in ('outras (especificar)', 'multiplas drogas mãe não soube reportar')
                    then 'Não especificado'
                else 
                    initcap(descricao_droga)
            end as droga,
        from {{ref('raw_pcsm_drogas')}}
    ),

    drogas_pacientes as (
        select
        id_paciente,
        id_droga,
        from {{ref('raw_pcsm_drogas_pacientes')}}
    ),

    drogas_pacientes_detalhado as (
        select 
            dp.id_paciente,
            p.numero_cpf_paciente as cpf,
            p.numero_cartao_saude as cns,
            d.droga,

        from drogas_pacientes dp
        left join drogas d on dp.id_droga = d.id_droga
        left join {{ref('raw_pcsm_pacientes')}} p on dp.id_paciente = p.id_paciente
    )


select * from drogas_pacientes_detalhado

