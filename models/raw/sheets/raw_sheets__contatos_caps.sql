{{
    config(
        schema="brutos_sheets",
        alias="contatos_caps",
        materialized="table",
        tags=["weekly"],
    )
}}

with
    source as (
        select

            ap,
            cnes,
            nome,
            tipo,
            -- Queremos criar um array para telefones e um para emails
            -- Problema: às vezes o campo vem nulo, e o BigQuery não
            -- aceita nulos em arrays
            -- Todos os exemplos online de array_agg() etc só possuem
            -- uma outra coluna, de chave/id. Achei assim mais simples
            -- > replace() ignora nulos (ex: replace(null, 'a', '') => null)
            -- > array_to_string ignora nulos (ex: [1, null, 2] => '1,2')
            -- > Usamos um caractere que presumidamente nunca vai aparecer
            --   em telefones e emails / pode ser removido sem problemas:
            --   "U+001F INFORMATION SEPARATOR ONE"
            -- > Convertemos o array pra string separado por ele, perdendo
            --   os nulos no caminho, e depois a string de volta pra array
            split(
                array_to_string([
                    replace(tel_1, '\u001f', ''),
                    replace(tel_2, '\u001f', ''),
                    replace(tel_3, '\u001f', '')
                ], '\u001f'),
                '\u001f'
            ) as tel,
            split(
                array_to_string([
                    replace(email_1, '\u001f', ''),
                    replace(email_2, '\u001f', '')
                ], '\u001f'),
                '\u001f'
            ) as email

        from {{ source("brutos_sheets_staging", "contatos_caps") }}
    ),

    no_empty_values as (
        select
            * except (tel, email),
            -- O truque acima de array -> string -> array deixa todos os arrays
            -- com tamanho 3, preenchidos com strings vazias; aqui filtramos isso
            array(
                select
                    trim(num_tel)
                from unnest(tel) as num_tel
                where trim(num_tel) != ''
            ) as tel,
            array(
                select
                    trim(email_addr)
                from unnest(email) as email_addr
                where trim(email_addr) != ''
            ) as email
        from source
    )

select *
from no_empty_values
