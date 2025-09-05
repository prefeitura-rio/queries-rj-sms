{{
    config(
        enabled=true,
        alias="gestacoes",
    )
}}

WITH 
    -- ------------------------------------------------------------
    -- Leitura dos Marcos Temporais de Gravidez
    -- ----------
    -- Marcos são eventos importantes relacionados ao acompanhamento do paciente
    -- ------------------------------------------------------------
    marcos AS (
        select *
        from {{ref('mart_linhas_cuidado__marcos_temporais')}}
        where linha_cuidado = 'gestacao'
    ),

    -- ------------------------------------------------------------
    -- Transições de Estado de Gestação
    -- ----------
    -- Usando função LEAD para criar casos sequenciais de transições de estado
    -- ------------------------------------------------------------
    transicoes_estado as (
        select
            cpf,
            tipo,
            data_diagnostico,
            LEAD(tipo) OVER ( PARTITION BY cpf ORDER BY data_diagnostico) AS tipo_seguinte,
            LEAD(data_diagnostico) OVER ( PARTITION BY cpf ORDER BY data_diagnostico) AS data_diagnostico_seguinte
        from marcos
    ),

    -- ------------------------------------------------------------
    -- Mapeamento de Casos de Transições de Estado
    -- ------------------------------------------------------------
    casos as (
        select
        *,
            case
                -- Caso Básico de Gestação Encerrada e Bem Documentada
                when (
                    tipo = 'Inicio de Gestação' and tipo_seguinte = 'Encerramento'
                ) then 'Encerramento Comprovado'

                -- Caso de uma Gestação Em Andamento
                when (
                    (
                        tipo = 'Inicio de Gestação'
                        and tipo_seguinte is null
                    )
                    and (
                        date_diff (
                            current_date(),
                            data_diagnostico,
                            day
                        ) < 300
                    )
                ) then 'Em Andamento'

                -- Caso de uma Gestação Encerrada Administrativamente
                when (
                    (
                        tipo = 'Inicio de Gestação'
                        and tipo_seguinte is null
                    )
                    and (
                        date_diff (
                            current_date(),
                            data_diagnostico,
                            day
                        ) >= 300
                    )
                ) then 'Encerramento Inferido'

                -- Caso de uma mesma gestação Duas Vezes Documentada como Encerrada no PEP
                when (
                    (
                        tipo = 'Encerramento'
                        and tipo_seguinte = 'Encerramento'
                    )
                    and (
                        date_diff (
                            data_diagnostico_seguinte,
                            data_diagnostico,
                            day
                        ) <= 60
                    )
                ) then 'Repetição'

                -- Caso de uma mesma gestação Duas Vezes Documentada como Iniciada no PEP
                when (
                    (
                        tipo = 'Inicio de Gestação'
                        and tipo_seguinte = 'Inicio de Gestação'
                    )
                    and (
                        date_diff (
                            data_diagnostico_seguinte,
                            data_diagnostico,
                            day
                        ) <= 60
                    )
                ) then 'Repetição'

                -- Caso em que existe um início de gestação implicito entre dois eventos de Encerramento
                -- PS.: Isso deveria ser impossível, a inativação de um CID só ocorre se ele já esteve ativo
                when (
                    (
                        tipo = 'Encerramento'
                        and tipo_seguinte = 'Encerramento'
                    )
                    and (
                        date_diff (
                            data_diagnostico_seguinte,
                            data_diagnostico,
                            day
                        ) > 60
                    )
                ) then 'Gravidez Oculta'

                -- Caso em que existe um encerramento implicito entre dois eventos de Início de Gestação
                -- PS.: Isso pode ser um bug no algoritmo de marcos temporais
                when (
                    (tipo = 'Inicio de Gestação' and tipo_seguinte = 'Inicio de Gestação') and 
                    (date_diff(data_diagnostico_seguinte, data_diagnostico, day) > 60)
                ) then 'Gravidez Oculta'

                else 'Transição sem Informação'
            end as tipo_transicao
        from transicoes_estado
    ),

    -- ------------------------------------------------------------
    -- Filtragem de Casos
    -- ------------------------------------------------------------
    -- Filtragem para remover casos de repetição de encerramento
    -- ------------------------------------------------------------
    casos_filtrados as (
        select *
        from casos
        where tipo_transicao not in (
            'Repetição',
            'Transição sem Informação'
        )
    )
select *
from casos_filtrados
order by cpf, data_diagnostico