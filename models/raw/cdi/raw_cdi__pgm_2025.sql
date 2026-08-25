{{ config(
    schema = "brutos_cdi",
    alias  = "pgm_2025",
    materialized = "table"
) }}

select
  {{ normalize_null("regexp_replace(trim(processo_rio__sei), r'[\\n\\r]', '')") }} as processo_rio,

  {{ normalize_null("regexp_replace(trim(procurador_a), r'[\\n\\r\\t]+', '')") }} as procurador,

  {{ normalize_null("regexp_replace(trim(requerente), r'[\\n\\r\\t]+', '')") }} as requerente,

  {{ normalize_null("regexp_replace(trim(processo_judicial), r'[\\n\\r]', '')") }} as processo_judicial,

  {{ normalize_null("regexp_replace(trim(origem), r'[\\n\\r\\t]+', '')") }} as origem,

  {{ cdi_parse_date('data_de_entrada', 'processo_rio__sei', 'processo_judicial') }} as data_entrada,

  {{ cdi_parse_date('data_de_saida', 'processo_rio__sei', 'processo_judicial') }} as data_saida,

  {{ cdi_parse_date('data_de_saida_para_pgm', 'processo_rio__sei', 'processo_judicial') }} as data_saida_pgm,

  {{ cdi_parse_date('prazo', 'processo_rio__sei', 'processo_judicial') }} as prazo,

  {{ normalize_null("trim(mes_ano)") }} as mes_ano,

  upper(trim({{ normalize_null('sexo') }})) as sexo,

  {{ normalize_null("regexp_replace(trim(idade), r'[\\n\\r\\t]+', '')") }} as idade,

  upper(
    case
      when regexp_contains(
        lower(trim(regexp_replace(regexp_replace(normalize({{ normalize_null('hospital_de_origem') }}, NFD), r'\pM', ''), r'\s+', ' '))),
        r'do+m'
      ) then 'Domicilio'
      else trim(regexp_replace(regexp_replace(normalize({{ normalize_null('hospital_de_origem') }}, NFD), r'\pM', ''), r'\s+', ' '))
    end
  ) as hospital_origem,

  trim({{ normalize_null('cap') }}) as cap,

  trim({{ normalize_null('erro_medico') }}) as erro_medico,

  trim({{ normalize_null('acp') }}) as acp,

  trim({{ normalize_null('multa_bloqueio_de_verba_indenizacao') }}) as tipo_indenizacao,

  {{ normalize_null('valor') }} as valor,

  trim({{ normalize_null('mandado_de_prisao') }}) as mandado_prisao,

  trim({{ normalize_null('crime_de_desobediencia') }}) as crime_desobediencia,

  regexp_replace(trim({{ normalize_null('patologia__assunto') }}), r'\s+', ' ') as patologia_assunto,

  regexp_replace(trim({{ normalize_null('solicitacao') }}), r'\s+', ' ') as solicitacao,

  regexp_replace(trim({{ normalize_null('sintese_de_solicitacao') }}), r'\s+', ' ') as sintese_solicitacao,

  {{ normalize_null("trim(setor_responsavel_pela_resposta)") }} as setor_responsavel,

  safe_cast({{ normalize_null('prazo__dias') }} as int64) as prazo_dias,

  case
    when lower(trim(cast(situacao as string))) in ('#ref!', '#value!')
      then null
    else {{ normalize_null("regexp_replace(trim(situacao), r'\\s+', ' ')") }}
  end as situacao,

  trim({{ normalize_null('pendencias') }}) as pendencias,

  regexp_replace(trim({{ normalize_null('observacoes') }}), r'\s+', ' ') as observacoes

from {{ source("brutos_cdi_staging", "pgm_2025") }}