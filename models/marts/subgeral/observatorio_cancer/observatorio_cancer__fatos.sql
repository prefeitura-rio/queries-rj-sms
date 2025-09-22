with

sisreg as (
    select 
        "SISREG" as sistema,
        id_solicitacao as id_episodio,
        paciente_cns,
        paciente_nome,
        date(data_solicitacao) as data_solicitacao,
        date(data_execucao) as data_realizacao,
        upper(trim(REGEXP_REPLACE(NORMALIZE(procedimento, NFD), r'\pM', ''))) as procedimento,
        id_cnes_unidade_solicitante,
        id_cnes_unidade_executante,

    from {{ ref('mart_sisreg__solicitacoes') }}
    where 1=1
        and data_solicitacao >= '2025-06-01' 
        and id_procedimento_sisreg in (
            "1407035", "0816013", "2300036",
            "0820029", "0820058", "2018205", 
            "3100093", "0710188", "9283006",
            "0705102", "3512043",	"2018206",	
            "0225039"
        )
),

siscan as (
    select 
        "SISCAN" as sistema,
        protocolo_id as id_episodio,
        paciente_cns,
        paciente_nome,
        data_solicitacao,
        data_realizacao,
        upper(trim(REGEXP_REPLACE(NORMALIZE(mamografia_tipo, NFD), r'\pM', ''))) as procedimento,
        unidade_solicitante_id_cnes, 
        unidade_prestadora_id_cnes,

        struct (
            exame_id,
            data_liberacao_resultado,
            mama_direita_classif_radiologica,
            mama_esquerda_classif_radiologica,
            mamografia_recomendacoes
        ) as dados_mamografia

    from {{ ref("raw_siscan_web__laudos") }}
    where 1=1
    and paciente_cns in (
        select paciente_cns from sisreg
    ) --implementar uma tentativa pelo cns do hci tbm
),

episodios as (
    select
        sisreg.*,
        struct (
            cast(null as string) as exame_id,
            date(null) as data_liberacao_resultado,
            cast(null as string) as mama_direita_classif_radiologica,
            cast(null as string) as mama_esquerda_classif_radiologica,
            cast(null as string) as mamografia_recomendacoes
        ) as dados_mamografia
    from sisreg

    union all

    select *
    from siscan
),

pacientes_suspeitos as (
    select paciente_cns
    from siscan
    where 1=1
    and (
        dados_mamografia.mama_direita_classif_radiologica in (
            "Categoria 4 - achados mamográficos suspeitos",
            "Categoria 5 - achados mamográficos altamente suspeitos"
            )
        or 
        dados_mamografia.mama_esquerda_classif_radiologica in (
            "Categoria 4 - achados mamográficos suspeitos",
            "Categoria 5 - achados mamográficos altamente suspeitos"
            )
    )
),

flag_pacientes_suspeitos as (
    select
        episodios.*,
        case
            when paciente_cns in (select paciente_cns from pacientes_suspeitos) then "SIM"
            else "NAO" 
        end as indicador_historico_suspeito 
    from episodios 
)

select *
from flag_pacientes_suspeitos
where indicador_historico_suspeito = "SIM"
