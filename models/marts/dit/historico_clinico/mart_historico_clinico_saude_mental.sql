{{ 
    config(
        schema="saude_historico_clinico",
        alias = "saude_mental_epsodios",
        materialized = "table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    ) 
}}

with 

    estabelecimentos as (
        select
            id_cnes, 
            tipo_sms,
            {{proper_estabelecimento('nome_acentuado')}} as nome,
        from {{ref('dim_estabelecimento')}}
    ),

    pacientes as (
        select 
            cpf,
            dados.id_paciente as id_paciente,
            dados.data_nascimento as data_nascimento
        from {{ref('mart_historico_clinico__paciente')}}
    ),

    acolhimentos as (
        select
            {{ dbt_utils.generate_surrogate_key(
                [
                    'acolhimentos.id_acolhimento',
                    'acolhimentos.id_cnes',
                    'a.cpf'
                ]
                )
            }} as id_hci, -- Confirmar depois

            a.cpf as paciente_cpf, 

            struct(
                p.cpf,
                a.cns,
                p.id_paciente,
                p.data_nascimento
            ) as paciente,
            

            'Acolhimento' as tipo,
            cast(null as string) as subtipo,           
            a.acolhimentos.data_inicio as entrada_data,
            a.acolhimentos.data_termino as termino_data,
            
            -- acolhimento
            struct(
                a.acolhimentos.id_acolhimento,
                a.acolhimentos.turno,
                cast(a.acolhimentos.tipo_leito as string) as tipo_leito,
                a.acolhimentos.leito_ocupado as leito_ocupado
            ) as acolhimento,
            
            cast(null as struct<
                id_atividade int64,
                nome string
            >) as atividade_grupo,

            cast(null as struct<
                id_matriciamento int64,
                nome string,
                forma string,
                evolucao string
            >) as matriciamento,

            cast(null as struct<
                id_articulacao int64,
                nome string,
                forma string,
                evolucao string
            >) as articulacao,

            -- estabelecimento
            struct(
                acolhimentos.id_cnes as id_cnes, 
                coalesce(
                    a.acolhimentos.unidade_nome,
                    e.nome
                ) as nome,
                e.tipo_sms as estabelecimento_tipo
            ) as estabelecimento,

            struct(
                current_timestamp() as transformed_at,
                a.loaded_at as imported_at
            ) as metadados,
            cast(a.cpf as int64) as cpf_particao

        from {{ ref('int_historico_clinico__acolhimentos__pcsm')}} as a
        left join estabelecimentos e on a.acolhimentos.id_cnes = e.id_cnes
        left join pacientes p on a.cpf = p.cpf

        where a.cpf is not null
    ),

    atividade_grupo as (
        select
            {{ dbt_utils.generate_surrogate_key(
                [
                    'atividade_grupo.id_atividade',
                    'atividade_grupo.id_cnes',
                    'ag.cpf'
                ]
                )
            }} as id_hci, -- Confirmar depois

            ag.cpf as paciente_cpf, 
            struct(
                p.cpf,
                ag.cns,
                p.id_paciente,
                p.data_nascimento
            ) as paciente,

            'Atividade em grupo' as tipo,
            ag.atividade_grupo.tipo as subtipo,
            ag.atividade_grupo.data_inicio as entrada_data,
            cast(ag.atividade_grupo.data_termino as datetime) as termino_data,

            cast(null as struct<
                id_acolhimento int64,
                turno string,
                tipo_leito string,
                leito_ocupado boolean
            >) as acolhimento,
            
            struct(
                ag.atividade_grupo.id_atividade,
                ag.atividade_grupo.nome
            ) as atividade_grupo,

            cast(null as struct<
                id_matriciamento int64,
                nome string,
                forma string,
                evolucao string
            >) as matriciamento,

            cast(null as struct<
                id_articulacao int64,
                nome string,
                forma string,
                evolucao string
            >) as articulacao,

            -- estabelecimento
            struct(
                atividade_grupo.id_cnes as id_cnes, 
                coalesce(
                    ag.atividade_grupo.unidade_nome,
                    e.nome
                ) as nome,
                e.tipo_sms as estabelecimento_tipo
            ) as estabelecimento,

            struct(
                current_timestamp() as transformed_at,
                cast(null as timestamp) as imported_at
            ) as metadados,
            cast(ag.cpf as int64) as cpf_particao

        from {{ ref('int_historico_clinico__atividade_grupo__pcsm')}} as ag
        left join estabelecimentos e on ag.atividade_grupo.id_cnes = e.id_cnes
        left join pacientes p on ag.cpf = p.cpf

        where ag.cpf is not null
    ),

    matriciamentos as (
        select 
            {{ dbt_utils.generate_surrogate_key(
                [
                    'matriciamentos.id_matriciamento',
                    'matriciamentos.id_cnes',
                    'm.cpf'
                ]
                )
            }} as id_hci, -- Confirmar depois
            
            m.cpf as paciente_cpf,
            struct(
                p.cpf,
                m.cns,
                p.id_paciente,
                p.data_nascimento
            ) as paciente,

            'Matriciamento' as tipo,
            m.matriciamentos.tipo as subtipo,
            cast(m.matriciamentos.data_inicio as datetime) as entrada_data,
            cast(null as datetime) as termino_data,

            cast(null as struct<
                id_acolhimento int64,
                turno string,
                tipo_leito string,
                leito_ocupado boolean
            >) as acolhimento,

            cast(null as struct<
                id_atividade int64,
                nome string
            >) as atividade_grupo,

            struct(
                m.matriciamentos.id_matriciamento,
                m.matriciamentos.nome_matriciamento as nome,
                m.matriciamentos.forma,
                m.matriciamentos.evolucao
            ) as matriciamento,

            cast(null as struct<
                id_articulacao int64,
                nome string,
                forma string,
                evolucao string
            >) as articulacao,

            struct (
                m.matriciamentos.id_cnes as id_cnes,
                coalesce(
                    m.matriciamentos.nome_unidade,
                    e.nome
                ) as nome,
                e.tipo_sms as estabelecimento_tipo
            ) as estabelecimento,

            struct(
                current_timestamp() as transformed_at,
                loaded_at as imported_at
            ) as metadados,
            cast(m.cpf as int64) as cpf_particao

        from {{ ref('int_historico_clinico__matriciamento__pcsm')}} m
        left join pacientes p on m.cpf = p.cpf
        left join estabelecimentos e on m.matriciamentos.id_cnes = e.id_cnes

        where m.cpf is not null
    ),

    articulacoes as (
        select 
            {{ dbt_utils.generate_surrogate_key(
                [
                    'articulacoes.id_articulacao',
                    'articulacoes.id_cnes',
                    'a.cpf'
                ]
                )
            }} as id_hci, -- Confirmar depois

            a.cpf,
            
            struct(
                p.cpf,
                a.cns,
                p.id_paciente,
                p.data_nascimento
            ) as paciente,

            'Articulação' as tipo,
            a.articulacoes.tipo as subtipo,
            a.articulacoes.datahora_inicio as entrada_datahora,
            cast(a.articulacoes.datahora_termino as datetime) as termino_datahora,

            cast(null as struct<
                id_acolhimento int64,
                turno string,
                tipo_leito string,
                leito_ocupado boolean
            >) as acolhimento,

            cast(null as struct<
                id_atividade int64,
                nome string
            >) as atividade_grupo,

            cast(null as struct<
                id_matriciamento int64,
                nome string,
                forma string,
                evolucao string
            >) as matriciamento,

            struct(
                a.articulacoes.id_articulacao,
                a.articulacoes.nome, 
                a.articulacoes.forma,
                a.articulacoes.evolucao
            ) as articulacao,

            struct (
                a.articulacoes.id_cnes as id_cnes,
                coalesce(
                    a.articulacoes.nome_unidade,
                    e.nome
                ) as nome,
                e.tipo_sms as estabelecimento_tipo
            ) as estabelecimento,
            
            struct(
                current_timestamp() as transformed_at,
                loaded_at as imported_at
            ) as metadados,
            cast(a.cpf as int64) as cpf_particao

        from {{ref('int_historico_clinico__articulacao__pcsm')}} a
        left join pacientes p on a.cpf = p.cpf
        left join estabelecimentos e on a.articulacoes.id_cnes = e.id_cnes

        where a.cpf is not null
    )

select * from acolhimentos
union all
select * from atividade_grupo
union all 
select * from matriciamentos
union all 
select * from articulacoes