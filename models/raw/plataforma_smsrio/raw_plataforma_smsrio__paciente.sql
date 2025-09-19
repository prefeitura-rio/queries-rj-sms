{{
    config(
        alias="paciente",
        tags="smsrio"
    )
}}
with 
    source as (
        select * from {{ source('brutos_plataforma_smsrio_staging','sms_pacientes__tb_pacientes') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by id order by timestamp desc) = 1
    ),
    transform as (
        select 
            -- Chave Primária
            safe_cast({{process_null('id')}} as string) as id,
            
            -- Outras Chaves
            safe_cast({{process_null('cpf')}} as string) as cpf,
            safe_cast({{process_null('cns')}} as string) as cns,

            -- Informações Pessoais
            safe_cast({{process_null('nome')}} as string) as nome,
            safe_cast({{process_null('nome_mae')}} as string) as nome_mae,
            safe_cast({{process_null('nome_pai')}} as string) as nome_pai,
            safe_cast({{process_null('sexo')}} as string) as sexo,
            safe_cast({{process_null('obito')}} as string) as obito,
            safe_cast({{process_null('dt_obito')}} as date) as data_obito,
            safe_cast({{process_null('racaCor')}} as string) as raca_cor,

            -- Contato
            safe_cast({{process_null('email')}} as string) as email,
            safe_cast({{process_null('tp_email')}} as string) as tp_email,
            safe_cast({{process_null('telefone')}} as string) as telefone,
            safe_cast({{process_null('tp_telefone')}} as string) as tp_telefone,

            -- Nascimento
            safe_cast({{process_null('nacionalidade')}} as string) as nacionalidade,
            safe_cast({{process_null('dt_nasc')}} as date) as data_nascimento,
            safe_cast({{process_null('cod_mun_nasc')}} as string) as codigo_municipio_nascimento,
            safe_cast({{process_null('uf_nasc')}} as string) as uf_nascimento,
            safe_cast({{process_null('cod_pais_nasc')}} as string) as codigo_pais_nascimento,

            -- Endereço
            safe_cast({{process_null('end_tp_logrado_nm')}} as string) as endereco_tipo_logradouro,
            safe_cast({{process_null('end_cep')}} as string) as endereco_cep,
            safe_cast({{process_null('end_logrado')}} as string) as endereco_logradouro,
            safe_cast({{process_null('end_numero')}} as string) as endereco_numero,
            safe_cast({{process_null('end_comunidade')}} as string) as endereco_comunidade,
            safe_cast({{process_null('end_complem')}} as string) as endereco_complemento,
            safe_cast({{process_null('end_bairro')}} as string) as endereco_bairro,
            safe_cast({{process_null('cod_mun_res')}} as string) as endereco_municipio_codigo,
            safe_cast({{process_null('uf_res')}} as string) as endereco_uf,

            -- Metadata columns
            timestamp_add(datetime(timestamp({{process_null('timestamp')}}), 'America/Sao_Paulo'),interval 3 hour)  as updated_at,
            datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at
        from most_recent
    )
select * from transform