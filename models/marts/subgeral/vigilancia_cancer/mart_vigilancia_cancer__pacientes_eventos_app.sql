with

populacao_interesse as (
    select distinct paciente_cpf
    from {{ref ("mart_vigilancia_cancer__fatos")}}
    where
        data_solicitacao >= "2025-01-01"
        and (
                -- critérios de seleção SISCAN
                (
                    sistema_origem = "SISCAN"
                    and mama_esquerda_classif_radiologica in (
                        "Categoria 4 - achados mamográficos suspeitos",
                        "Categoria 5 - achados mamográficos altamente suspeitos",
                        "Categoria 6 - achados mamográficos"

                    )
                )

                or 

                -- critérios de seleção SISREG
                (
                    sistema_origem = "SISREG"
                    and procedimento in (
                        "MAMOGRAFIA  DIAGNOSTICA",
                        "BIOPSIA DE MAMA   LESAO PALPAVEL",
                        "BIOPSIA DE MAMA GUIADA POR USG",
                        "BIOPSIA DE MAMA POR ESTEREOTAXIA",

                        "ULTRASSONOGRAFIA MAMARIA BILATERAL PARA ORIENTAR BIOPSIA DE MAMA"
                    )
                )

                or 

                -- critérios de seleção SER
                (
                    sistema_origem = "SER"
                    and procedimento in (
                        "AMBULATORIO 1  VEZ   MASTOLOGIA  ONCOLOGIA"
                    )
                )

        )
)

select
    paciente_cpf
from {{ ref("mart_vigilancia_cancer__fatos") }}
where paciente_cpf in (
    select paciente_cpf
    from populacao_interesse
)