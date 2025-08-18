{{
    config(
        schema="intermediario_historico_clinico",
        alias="exames_cientificalab",
        materialized="table",
    )
}}

with
    solicitacoes as (
        select
            paciente_cpf,
            unidade as unidade_nome,
            id
        from {{ ref('raw_cientificalab__solicitacoes') }}
    ),

    exame as (
        select
            id,
            solicitacao_id,
            cod_apoio,
            data_assinatura
        from {{ ref('raw_cientificalab__exames') }}
    ),

    resultado as (
        select
            id,
            exame_id,
            resultado,
            unidade,
            valor_referencia_minimo,
            valor_referencia_maximo,
            valor_referencia_texto
        from {{ ref('raw_cientificalab__resultados') }}
    ),

    exames_com_resultados as (
        select
            s.paciente_cpf,
            case
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL RAPHAEL DE PAULA SOUZA' then '2273349'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL SALGADO FILHO' then '2296306'
                when upper(unidade_nome) = 'HOSP MATERNIDADE HERCULANO PINHEIRO' then '2270390'
                when upper(unidade_nome) = 'HOSP MATERNIDADE ALEXANDER FLEMING' then '2269945'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL BARATA RIBEIRO' then '2270242'
                when upper(unidade_nome) = 'HOSPITAL MATERNIDADE FERNANDO MAGALHÃES' then '2270714'
                when upper(unidade_nome) = 'HOSP. MAT. CARMELA DUTRA' then '2280248'
                when upper(unidade_nome) = 'COMPLEXO MIGUEL COUTO' then '2270269'
                when upper(unidade_nome) = 'INSTITUTO DR. PHILIPPE PINEL' then '2288362'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL SOUZA AGUIAR' then '2280183'
                when upper(unidade_nome) = 'PAQUETA' then '2277301'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL ALVARO RAMOS' then '2273187'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL DA PIEDADE' then '2269481'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL JESUS' then '2269341'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL LOURENÇO JORGE' then '2270609'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL FRANCISCO DA SILVA TELLES' then '2291266'
                when upper(unidade_nome) = 'CER CENTRO' then '6716911'
                when upper(unidade_nome) = 'H.M. NOSSA SENHORA DO LORETO' then '2269724'
                when upper(unidade_nome) = 'UPA - ROCINHA' then '6507409'
                when upper(unidade_nome) = 'HOSPITAL MUNICIPAL ROCHA MAIA' then '2273489'
                when upper(unidade_nome) = 'MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA' then '7027397'
                else unidade_nome 
            end as id_cnes,
            e.cod_apoio,
            e.data_assinatura,
            r.resultado,
            r.unidade,
            r.valor_referencia_minimo,
            r.valor_referencia_maximo,
            r.valor_referencia_texto
        from solicitacoes as s
        inner join exame as e on s.id = e.solicitacao_id
        inner join resultado as r on e.id = r.exame_id
    ),

    exame_deduplicado as (
        select *
        from exames_com_resultados
        qualify
            row_number() over (
                partition by paciente_cpf, id_cnes, cod_apoio, data_assinatura order by data_assinatura desc
            ) = 1
    ),

    exame_agg as (
        select
            paciente_cpf,
            array_agg(
                struct(
                    id_cnes,
                    cod_apoio as codigo_do_exame,
                    data_assinatura as data_do_exame,
                    resultado,
                    unidade,
                    valor_referencia_minimo,
                    valor_referencia_maximo,
                    valor_referencia_texto
                )
            ) as exames
        from exame_deduplicado
        group by
            paciente_cpf
    )

select * from exame_agg