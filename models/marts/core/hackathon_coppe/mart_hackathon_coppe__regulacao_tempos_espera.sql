{{
    config(
        materialized = "table",
        alias = "tempo_espera"
    )
}}
select 
    ano_marcacao,
    mes_marcacao,
    procedimento,
    n_execucoes,
    tme as tempo_medio_espera,
    te_mediano as tempo_espera_mediano,
    te_p90 as tempo_espera_90_percentil,
    desvio_padrao as tempo_espera_desvio_padrao,
    ic95_inf as intervalo_confianca_95_inferior,
    ic95_sup as intervalo_confianca_95_superior,
    tme_movel_3m as tempo_medio_espera_movel_3m,
    tme_movel_6m as tempo_medio_espera_movel_6m,
    tme_movel_12m as tempo_medio_espera_movel_12m

from {{source("projeto_sisreg_tme","tme_procedimento")}}
