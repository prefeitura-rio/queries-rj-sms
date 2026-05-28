with pacientes as (
    select
        -- id
        safe_cast(usuariocpf as int) as paciente_cpf,
        safe_cast(usuariocns as int) as paciente_cns,
        safe_cast(usuarionome as string) as paciente_nome,

        estabs.nome_acentuado as clinica_sf,
        estabs.area_programatica as clinica_sf_ap,
        estabs.telefone[SAFE_OFFSET(0)] as clinica_sf_telefone,

        -- proxy de atualização cadastral: data da solicitação mais recente no relatório TEA.
        safe_cast(solicitacaodatahora as timestamp) as data_atualizacao

    from {{ ref("raw_centralderegulacao_mysql__tea_relatorio") }}
    left join {{ ref("dim_estabelecimento") }} as estabs
    on safe_cast(unidaderefcnes as int) = safe_cast(id_cnes as int)
    where date(solicitacaodatahora) >= date '2024-01-01'
)

select * from pacientes
qualify row_number() over (
    partition by coalesce(safe_cast(paciente_cpf as string), safe_cast(paciente_cns as string))
    order by data_atualizacao desc
) = 1