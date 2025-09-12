{{
    config(
        enabled=true,
        schema="projeto_sisreg_reports",
        alias="oferta_programada__anomalia_profissional",
        materialized="incremental",
        partition_by={"field": "data_calculo_anomalia", "data_type": "DATE"},
    )
}}

{% set data_atual = run_started_at.strftime("%Y-%m-%d") %}

-- To do: considerar alterações nas cargas horárias (CNES) e AFASTAMENTOS
with

    -- Etapa 1: Identificar a data de partição mais recente
    -- Esta etapa encontra a data máxima de partição na tabela histórica
    particao_mais_recente as (
        select max(data_particao) as data_maxima
        from {{ ref("mart_sisreg__oferta_programada_serie_historica") }}
    ),

    -- Etapa 2: Definir intervalos de tempo relevantes
    -- Definimos datas para seis meses atrás, hoje, início e fim do próximo mês
    intervalos_de_tempo as (
        select
            date_sub(date('{{ data_atual }}'), interval 6 month) as seis_meses_atras,
            date('{{ data_atual }}') as hoje,
            date_trunc(
                date_add(date('{{ data_atual }}'), interval 1 month), month
            ) as inicio_proximo_mes,
            date_trunc(
                date_add(date('{{ data_atual }}'), interval 2 month), month
            ) as fim_proximo_mes
    ),

    -- Etapa 3: Agregar dados mensais históricos dos últimos seis meses
    -- Somamos o total de vagas para cada profissional, procedimento e estabelecimento
    -- por mês
    dados_mensais_historicos as (
        select
            profissional_executante_cpf,
            id_procedimento_interno,
            id_estabelecimento_executante,
            date_trunc(procedimento_vigencia_data, month) as data_mes,
            sum(vagas_todas_qtd) as vagas_totais
        from {{ ref("mart_sisreg__oferta_programada_serie_historica") }}
        where
            data_particao = (select data_maxima from particao_mais_recente)
            and procedimento_vigencia_data
            >= (select seis_meses_atras from intervalos_de_tempo)
            and procedimento_vigencia_data <= (select hoje from intervalos_de_tempo)

        group by
            profissional_executante_cpf,
            id_procedimento_interno,
            id_estabelecimento_executante,
            data_mes
    ),

    -- Etapa 4: Agregar vagas planejadas para o próximo mês
    -- Calculamos o total de vagas planejadas para o próximo mês
    vagas_proximo_mes_tb as (
        select
            profissional_executante_cpf,
            id_procedimento_interno,
            string_agg(distinct procedimento) as procedimento,
            id_estabelecimento_executante,
            string_agg(distinct estabelecimento) as estabelecimento,
            string_agg(distinct profissional_executante_nome) as profissional_nome,
            date_trunc(procedimento_vigencia_data, month) as data_mes,
            sum(vagas_todas_qtd) as vagas_proximo_mes
        from {{ ref("mart_sisreg__oferta_programada_serie_historica") }}
        where
            data_particao = (select data_maxima from particao_mais_recente)
            and procedimento_vigencia_data
            >= (select inicio_proximo_mes from intervalos_de_tempo)
            and procedimento_vigencia_data
            < (select fim_proximo_mes from intervalos_de_tempo)

        group by
            profissional_executante_cpf,
            id_procedimento_interno,
            id_estabelecimento_executante,
            data_mes
    ),

    -- Etapa 5: Calcular diferenças sequenciais nas vagas ao longo dos meses
    -- Calculamos a diferença de vagas entre meses consecutivos para detectar
    -- tendências
    diferencas_sequenciais as (
        select
            profissional_executante_cpf,
            id_procedimento_interno,
            id_estabelecimento_executante,
            data_mes,
            vagas_totais,
            vagas_totais - lag(vagas_totais) over (
                partition by
                    profissional_executante_cpf,
                    id_procedimento_interno,
                    id_estabelecimento_executante
                order by data_mes
            ) as diferenca_sequencial
        from dados_mensais_historicos
    ),

    -- Etapa 6: Agregar diferenças em arrays por profissional, procedimento e
    -- estabelecimento
    -- Agrupamos as diferenças sequenciais em arrays para facilitar o cálculo da
    -- mediana
    diferencas_agregadas as (
        select
            profissional_executante_cpf,
            id_procedimento_interno,
            id_estabelecimento_executante,
            array_agg(data_mes order by data_mes) as meses,
            array_agg(vagas_totais order by data_mes) as vagas_por_mes,
            array_agg(
                diferenca_sequencial ignore nulls order by data_mes
            ) as diferencas_por_mes
        from diferencas_sequenciais
        group by
            profissional_executante_cpf,
            id_procedimento_interno,
            id_estabelecimento_executante
    ),

    -- Etapa 7: Calcular a mediana das diferenças se houver dados suficientes
    -- Se tivermos pelo menos três diferenças, calculamos a mediana
    calculos_mediana as (
        select
            *,
            if(
                array_length(diferencas_por_mes) >= 3,
                {{ calculate_median_sql("diferencas_ordenadas") }},
                null
            ) as mediana_diferenca,
            vagas_por_mes[offset(array_length(vagas_por_mes) - 1)] as vagas_ultimo_mes
        from
            (
                select
                    *,
                    array(
                        select val from unnest(diferencas_por_mes) as val order by val
                    ) as diferencas_ordenadas
                from diferencas_agregadas
            )
    ),

    -- Etapa 8: Filtrar dados onde os cálculos da mediana são possíveis
    -- Eliminamos registros sem mediana calculável
    dados_filtrados as (
        select * from calculos_mediana where mediana_diferenca is not null
    ),

    -- Etapa 9: Calcular desvios absolutos da mediana
    -- Calculamos o desvio absoluto de cada diferença em relação à mediana
    desvios_absolutos as (
        select
            *,
            array(
                select abs(val - mediana_diferenca)
                from unnest(diferencas_ordenadas) as val
            ) as desvios
        from dados_filtrados
    ),

    -- Etapa 10: Calcular o Desvio Absoluto Mediano (MAD)
    -- O MAD é usado como medida robusta de variabilidade
    calculos_mad as (
        select *, {{ calculate_median_sql("desvios_ordenados") }} as mad
        from
            (
                select
                    *,
                    array(
                        select val from unnest(desvios) as val order by val
                    ) as desvios_ordenados
                from desvios_absolutos
            )
    ),

    -- Etapa 11: Preparar dados para detecção de anomalias calculando a diferença
    -- do próximo mês
    -- Calculamos a diferença esperada para o próximo mês e preparamos os dados
    -- para análise
    preparacao_anomalia as (
        select
            calculos_mad.*,
            estabelecimento,
            profissional_nome,
            procedimento,
            vagas_proximo_mes_tb.vagas_proximo_mes,
            vagas_proximo_mes_tb.vagas_proximo_mes
            - calculos_mad.vagas_ultimo_mes as diferenca_proximo_mes
        from calculos_mad
        left join
            vagas_proximo_mes_tb using (
                profissional_executante_cpf,
                id_procedimento_interno,
                id_estabelecimento_executante
            )
        where
            (vagas_proximo_mes_tb.vagas_proximo_mes - calculos_mad.vagas_ultimo_mes) < 0
    ),

    -- Etapa 12: Calcular pontuações de anomalia para desvios significativos
    -- Pontuamos as anomalias com base no desvio em relação à mediana e ao MAD
    -- ajustado para normalidade
    pontuacoes_anomalia as (
        select
            *,
            abs(diferenca_proximo_mes - mediana_diferenca)
            / (mad * 1.4826) as pontuacao_anomalia
        from preparacao_anomalia
        where mad is not null and mad != 0
    ),

    -- Etapa Final: Selecionar anomalias significativas com pontuação de anomalia
    -- maior
    -- que 3
    -- Filtramos para mostrar apenas as anomalias mais significativas
    final as (
        select
            id_estabelecimento_executante as id_cnes,
            estabelecimento,
            profissional_executante_cpf as profissional_cpf,
            profissional_nome,
            id_procedimento_interno as id_procedimento,
            procedimento,
            meses as meses_analisados,
            vagas_por_mes as historico_vagas_ofertadas,
            vagas_proximo_mes as vagas_programadas_proximo_mes,
            diferenca_proximo_mes as deficit_vagas_proximo_mes,
            date('{{ data_atual }}') as data_calculo_anomalia

        from pontuacoes_anomalia
        where pontuacao_anomalia > 3
        order by id_estabelecimento_executante, pontuacao_anomalia desc
    )

select *
from final
{% if is_incremental() %}
    where data_calculo_anomalia > (select max(data_calculo_anomalia) from {{ this }})
{% endif %}
