with
    estabelecimentos_std as (
        select
            gid,
            cnes,
            nome_estabelecimento,
            case
                when regexp_contains(sigla, r'UPA[A-Z-a-z-0-9]')
                then 'UPA'
                when regexp_contains(sigla, r'CER[A-Z-a-z-0-9]')
                then 'CER'
                when regexp_contains(sigla, r'HM[A-Z-a-z-0-9]')
                then 'HM'
                when regexp_contains(sigla, r'M[A-Z-a-z-0-9]')
                then 'M'
                else sigla
            end as sigla
        from {{ ref("raw_prontuario_vitai__m_estabelecimento") }}
    )
select *
from estabelecimentos_std
