{{
    config(
        alias="elegiveis"
    )
}}

with 

cadastros_por_unidade as (
  select
    cpf as paciente_id,
    id_cnes as unidade_id,
    ine_equipe as equipe_id,

    case
      when date_diff(date(current_date()), date(data_nascimento), year) < 2 then '0-2'
      when date_diff(date(current_date()), date(data_nascimento), year) < 6 then '3-6'
      when date_diff(date(current_date()), date(data_nascimento), year) < 13 then '7-13'
      when date_diff(date(current_date()), date(data_nascimento), year) < 18 then '14-18'
      when date_diff(date(current_date()), date(data_nascimento), year) < 45 then '19-45'
      when date_diff(date(current_date()), date(data_nascimento), year) < 65 then '45-65'
      else '66+'
    end as faixa_etaria,

    sexo,
    raca_cor,
    escolaridade,
    territorio_social,
    vulnerabilidade_social,

    updated_at as updated_at
  from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
  where
    ap = '22'
    and cpf is not null
    and ine_equipe is not null
),

enderecos_por_pessoa as (
  select 
    cpf as paciente_id,
    latitude as endereco_latitude,
    longitude as endereco_longitude,
    score as endereco_score
  from {{ source('brutos_hackathon_anthropic','localizacao') }}
  qualify row_number() over (
    partition by paciente_id
    order by endereco_score desc
  ) = 1
),

cadastros as (
  select *
  from cadastros_por_unidade
  qualify row_number() over (
    partition by paciente_id
    order by updated_at desc
  ) = 1
),

cadastros_com_endereco as (
  select 
    cadastros.*,
    enderecos_por_pessoa.endereco_latitude,
    enderecos_por_pessoa.endereco_longitude,
    enderecos_por_pessoa.endereco_score
  from cadastros
  inner join enderecos_por_pessoa using (paciente_id)
),

parametros_ruido as (
  select
    *,

    -- Números pseudoaleatórios determinísticos entre 0 e 1
    mod(
      abs(farm_fingerprint(concat(cast(paciente_id as string), '|hackathon_anthropic|ruido_latlong|u1'))),
      1000000
    ) / 1000000.0 as ruido_u1,

    mod(
      abs(farm_fingerprint(concat(cast(paciente_id as string), '|hackathon_anthropic|ruido_latlong|u2'))),
      1000000
    ) / 1000000.0 as ruido_u2

  from cadastros_com_endereco
),

enderecos_com_ruido_calculado as (
  select
    *,

    -- Raio máximo do ruído em metros
    100.0 as raio_maximo_ruido_metros,

    -- Distância aleatória dentro do círculo de raio 50m.
    -- O sqrt garante distribuição mais uniforme dentro da área do círculo.
    sqrt(ruido_u1) * 100.0 as ruido_distancia_metros,

    -- Ângulo aleatório entre 0 e 2π
    2 * acos(-1) * ruido_u2 as ruido_angulo_radianos

  from parametros_ruido
),

enderecos_anonimizados as (
  select
    *,

    endereco_latitude
      + (
        ruido_distancia_metros * cos(ruido_angulo_radianos)
      ) / 111320.0 as endereco_latitude_com_ruido,

    endereco_longitude
      + (
        ruido_distancia_metros * sin(ruido_angulo_radianos)
      ) / (
        111320.0 * cos(endereco_latitude * acos(-1) / 180.0)
      ) as endereco_longitude_com_ruido

  from enderecos_com_ruido_calculado
),

cadastros_com_endereco_ruidoso as (
  select
    paciente_id,
    equipe_id,
    unidade_id,

    faixa_etaria,
    sexo,
    raca_cor,
    escolaridade,
    territorio_social,
    vulnerabilidade_social,

    ST_GEOGPOINT(endereco_longitude, endereco_latitude) as endereco_original,
    ST_GEOGPOINT(endereco_longitude_com_ruido, endereco_latitude_com_ruido) as endereco_ruidoso

  from enderecos_anonimizados
),

pacientes_randomizados as (
  select
    *,
    row_number() over (
      partition by equipe_id
      order by rand()
    ) as endereco_random_id
  from cadastros_com_endereco_ruidoso
),

enderecos_randomizados as (
  select
    equipe_id,
    endereco_ruidoso,
    row_number() over (
      partition by equipe_id
      order by rand()
    ) as endereco_random_id
  from cadastros_com_endereco_ruidoso
)

SELECT
  {{ anonimize('p.paciente_id', "'hackathon_anthropic'") }} as paciente_id,
  {{ anonimize('p.equipe_id', "'hackathon_anthropic'") }} as equipe_id,
  {{ anonimize('p.unidade_id', "'hackathon_anthropic'") }} as unidade_id,

  faixa_etaria,
  sexo,
  raca_cor,
  escolaridade,
  territorio_social,
  vulnerabilidade_social,

  e.endereco_ruidoso as endereco,

  struct(
    p.paciente_id as cpf,
    p.equipe_id as ine,
    p.unidade_id as cnes,
    p.endereco_original as endereco,
    p.endereco_ruidoso as endereco_ruidoso,
    {{ random_int('p.paciente_id', 7, "'hackathon_anthropic'") }} as shift_dias
  ) as original

FROM pacientes_randomizados p
  left join enderecos_randomizados e
    on p.equipe_id = e.equipe_id
    and p.endereco_random_id = e.endereco_random_id