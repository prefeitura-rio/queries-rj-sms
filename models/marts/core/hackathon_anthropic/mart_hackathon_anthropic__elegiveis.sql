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
      when date_diff(date(current_date()), date(data_nascimento), year) < 6 then '0-6'
      when date_diff(date(current_date()), date(data_nascimento), year) < 18 then '6-18'
      when date_diff(date(current_date()), date(data_nascimento), year) < 45 then '19-45'
      when date_diff(date(current_date()), date(data_nascimento), year) < 65 then '45-65'
      else '66+'
    end as faixa_etaria,

    sexo,

    case
      when raca_cor in ('Branca', 'Parda', 'Preta') then raca_cor
      else 'Outros'
    end as raca_cor,

    ifnull(territorio_social or vulnerabilidade_social,false) as situacao_vulnerabilidade,

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
    situacao_vulnerabilidade,

    ST_GEOGPOINT(endereco_longitude, endereco_latitude) as endereco_original,
    ST_GEOGPOINT(endereco_longitude_com_ruido, endereco_latitude_com_ruido) as endereco_ruidoso

  from enderecos_anonimizados
),

-- Contar pacientes por equipe para filtrar equipes pequenas
contagem_por_equipe as (
  select
    equipe_id,
    count(*) as total_pacientes
  from cadastros_com_endereco_ruidoso
  group by 1
),

-- Filtrar apenas equipes com pelo menos 2000 pacientes
equipes_elegiveis as (
  select equipe_id
  from contagem_por_equipe
  where total_pacientes >= 2000
),

-- Filtrar cadastros para incluir apenas equipes elegíveis
cadastros_filtrados as (
  select c.*
  from cadastros_com_endereco_ruidoso c
  inner join equipes_elegiveis e using (equipe_id)
),

-- Criar lista única de equipes originais para mapeamento
equipes_originais as (
  select
    equipe_id,
    row_number() over (order by equipe_id) as posicao_original
  from (select distinct equipe_id from cadastros_filtrados)
),

-- Criar lista de equipes destino embaralhada (mesma lista de IDs, mas em ordem aleatória)
equipes_destino as (
  select
    equipe_id as equipe_id_destino,
    row_number() over (order by farm_fingerprint(concat(cast(equipe_id as string), '|hackathon_anthropic|shuffle'))) as posicao_destino
  from (select distinct equipe_id from cadastros_filtrados)
),

-- Mapear 1:1 cada equipe original para uma equipe destino
-- Garantindo que cada equipe destino recebe exatamente uma equipe original
mapeamento_equipes as (
  select
    o.equipe_id as equipe_id_original,
    d.equipe_id_destino
  from equipes_originais o
  inner join equipes_destino d on o.posicao_original = d.posicao_destino
),

-- Sample de 2000 pacientes por equipe
pacientes_sampled as (
  select
    *,
    row_number() over (
      partition by equipe_id
      order by rand()
    ) as rn
  from cadastros_filtrados
  qualify rn <= 2000
),

-- Criar pool de endereços por equipe ORIGINAL (antes do remapeamento)
-- Cada equipe terá até 2000 endereços disponíveis
pool_enderecos_por_equipe_original as (
  select
    equipe_id as equipe_id_original,
    endereco_ruidoso,
    row_number() over (
      partition by equipe_id
      order by rand()
    ) as endereco_id
  from pacientes_sampled
),

-- Aplicar mapeamento de equipes
pacientes_com_equipe_randomizada as (
  select
    p.paciente_id,
    m.equipe_id_destino as equipe_id,
    p.unidade_id,
    p.faixa_etaria,
    p.sexo,
    p.raca_cor,
    p.situacao_vulnerabilidade,
    p.endereco_original,
    p.equipe_id as equipe_id_original,  -- guardar a equipe original para referência
    row_number() over (
      partition by m.equipe_id_destino
      order by rand()
    ) as endereco_random_id  -- posição do paciente na nova equipe
  from pacientes_sampled p
  inner join mapeamento_equipes m on p.equipe_id = m.equipe_id_original
),

-- Para cada equipe destino, criar pool de endereços disponíveis
-- Os endereços vêm da equipe ORIGINAL correspondente
pool_enderecos_por_equipe_destino as (
  select
    m.equipe_id_destino as equipe_id,
    pe.endereco_ruidoso,
    pe.endereco_id
  from pool_enderecos_por_equipe_original pe
  inner join mapeamento_equipes m on pe.equipe_id_original = m.equipe_id_original
)

SELECT
  {{ anonimize('p.paciente_id', "'hackathon_anthropic'") }} as paciente_id,
  {{ anonimize('p.equipe_id', "'hackathon_anthropic'") }} as equipe_id,
  {{ anonimize('p.unidade_id', "'hackathon_anthropic'") }} as unidade_id,

  faixa_etaria,
  sexo,
  raca_cor,
  situacao_vulnerabilidade,

  e.endereco_ruidoso as endereco,

  struct(
    p.paciente_id as cpf,
    p.equipe_id_original as ine,  -- usar equipe original no struct
    p.unidade_id as cnes,
    p.endereco_original as endereco,
    e.endereco_ruidoso as endereco_ruidoso,  -- endereço vem da equipe destino
    {{ random_int('p.paciente_id', 7, "'hackathon_anthropic'") }} as shift_dias
  ) as original

FROM pacientes_com_equipe_randomizada p
  left join pool_enderecos_por_equipe_destino e
    on p.equipe_id = e.equipe_id
    and p.endereco_random_id = e.endereco_id