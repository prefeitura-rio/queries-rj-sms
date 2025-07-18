{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_vinculo_trabalho",
        materialized="table",
        tags=["raw", "pcsm"],
        description="Tipos de deficiências que os pacientes podem ter, segundo arquivo de configuração .ini do sistema PCSM."
    )
}} 

select codigo, descricao 
  from unnest([
        struct('I' as codigo, 'Informal' as descricao),
        struct('A', 'Trabalho assistido'),
        struct('F', 'Formal')
  ]) as tipos_vinculo_trabalho