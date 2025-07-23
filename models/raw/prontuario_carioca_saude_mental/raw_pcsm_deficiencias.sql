{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="deficiencias",
        materialized="table",
        tags=["raw", "pcsm"],
        description="Tipos de deficiências que os pacientes podem ter, segundo arquivo de configuração .ini do sistema PCSM."
    )
}} 

select codigo, descricao, current_timestamp() as transformed_at  
  from unnest([
        struct(1 as codigo, 'Auditiva' as descricao),
        struct(2, 'Motora'),
        struct(3, 'Intelectual'),
        struct(4, 'Visual')
  ]) as deficiencias