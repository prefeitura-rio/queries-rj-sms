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
-- field name	mode	type	description
-- id	NULLABLE	STRING	
-- mpi	NULLABLE	STRING	
-- cns	NULLABLE	STRING	
-- cpf	NULLABLE	STRING	
-- nome	NULLABLE	STRING	
-- nome_social	NULLABLE	STRING	
-- nome_mae	NULLABLE	STRING	
-- nome_pai	NULLABLE	STRING	
-- dt_nasc	NULLABLE	STRING	
-- sexo	NULLABLE	STRING	
-- racaCor	NULLABLE	STRING	
-- codigoRacaCor	NULLABLE	STRING	
-- tp_sangue	NULLABLE	STRING	
-- end_tp_logrado_nm	NULLABLE	STRING	
-- end_tp_logrado_cod	NULLABLE	STRING	
-- end_logrado	NULLABLE	STRING	
-- end_numero	NULLABLE	STRING	
-- end_comunidade	NULLABLE	STRING	
-- end_complem	NULLABLE	STRING	
-- end_bairro	NULLABLE	STRING	
-- end_cod_bairro	NULLABLE	STRING	
-- end_cep	NULLABLE	STRING	
-- cod_mun_nasc	NULLABLE	STRING	
-- munic_nasc	NULLABLE	STRING	
-- uf_nasc	NULLABLE	STRING	
-- cod_pais_nasc	NULLABLE	STRING	
-- pais_nasc	NULLABLE	STRING	
-- nacionalidade	NULLABLE	STRING	
-- cod_mun_res	NULLABLE	STRING	
-- munic_res	NULLABLE	STRING	
-- uf_res	NULLABLE	STRING	
-- telefone	NULLABLE	STRING	
-- tp_telefone	NULLABLE	STRING	
-- tel_resid	NULLABLE	STRING	
-- tel_cel	NULLABLE	STRING	
-- tp_email	NULLABLE	STRING	
-- email	NULLABLE	STRING	
-- ap	NULLABLE	STRING	
-- cnes_res	NULLABLE	STRING	
-- nome_ub_res	NULLABLE	STRING	
-- cod_area	NULLABLE	STRING	
-- cod_microa	NULLABLE	STRING	
-- lat	NULLABLE	STRING	
-- lng	NULLABLE	STRING	
-- dnv	NULLABLE	STRING	
-- nis	NULLABLE	STRING	
-- rg	NULLABLE	STRING	
-- rg_dt	NULLABLE	STRING	
-- rg_emis	NULLABLE	STRING	
-- rg_uf	NULLABLE	STRING	
-- cnh	NULLABLE	STRING	
-- cnh_dt	NULLABLE	STRING	
-- cnh_uf	NULLABLE	STRING	
-- doc_cert_tp	NULLABLE	STRING	
-- doc_cert_cart	NULLABLE	STRING	
-- doc_cert_livro	NULLABLE	STRING	
-- doc_cert_folha	NULLABLE	STRING	
-- doc_cert_termo	NULLABLE	STRING	
-- doc_cert_dt	NULLABLE	STRING	
-- doc_cert_uf	NULLABLE	STRING	
-- doc_cert_cod_mun	NULLABLE	STRING	
-- doc_cert_matricula	NULLABLE	STRING	
-- tit_eleit	NULLABLE	STRING	
-- tit_eleit_zona	NULLABLE	STRING	
-- tit_eleit_sec	NULLABLE	STRING	
-- ctps_numero	NULLABLE	STRING	
-- ctps_serie	NULLABLE	STRING	
-- ctps_dt	NULLABLE	STRING	
-- pass	NULLABLE	STRING	
-- pass_pais	NULLABLE	STRING	
-- pass_dt_val	NULLABLE	STRING	
-- pass_dt_exp	NULLABLE	STRING	
-- dt_cadastro	NULLABLE	STRING	
-- obito	NULLABLE	STRING	
-- dt_obito	NULLABLE	STRING	
-- counter	NULLABLE	STRING	
-- cpf_valido	NULLABLE	STRING	
-- origem	NULLABLE	STRING	
-- cns_tp	NULLABLE	STRING	
-- cns_dt_ativa	NULLABLE	STRING	
-- ativo	NULLABLE	STRING	
-- timestamp	NULLABLE	STRING	
-- datalake_loaded_at	NULLABLE	STRING
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