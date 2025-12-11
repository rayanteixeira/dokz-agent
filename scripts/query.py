def getRegrasRepasse():
    return """
        SELECT
            --concat(b.cd_regra, b.nr_sequencia) 		CD_REGRA_NR_SEQ_CRITERIO,
            B.CD_REGRA                              CD_REGRA_R,
            B.NR_SEQUENCIA 							NR_SEQ_CRITERIO_R,
            B.IE_FORMA_CALCULO 						IE_FORMA_CALCULO_R,
            B.TX_ANESTESISTA 						TX_ANESTESISTA_R,
            nvl(B.TX_MEDICO,0)								TX_MEDICO_R,
            B.TX_MATERIAIS 							TX_MATERIAIS_R,
            B.TX_AUXILIARES 						TX_AUXILIARES_R,
            B.TX_CUSTO_OPERACIONAL 					TX_CUSTO_OPERACIONAL_R,
            NVL(VL_LIMITE,0) 						VL_LIMITE_R,
            NVL((B.IE_HONORARIO),'N') 				IE_HONORARIO_R,
            NVL(B.TX_PROCEDIMENTO, 0)				TX_PROCEDIMENTO_R,
            NVL((B.IE_PERC_PACOTE), 'N') 			IE_PERC_PACOTE_R,
            NVL((B.IE_REPASSE_CALC), 'S') 			IE_REPASSE_CALC_R,
            B.IE_TIPO_ATEND_CALC 					IE_TIPO_ATEND_CALC_R,
            B.VL_REPASSE 							VL_REPASSE_R,
            NR_SEQ_REGRA_PRIOR_REPASSE 				NR_SEQ_REGRA_PRIOR_R,
            B.VL_MINIMO 							VL_MINIMO_R,
            NVL((B.IE_LIMITE_QTDADE),'N') 			IE_LIMITE_QTDADE_R,
            IE_CAMPO_BASE_VL_REPASSE 				IE_CAMPO_BASE_VL_REPASSE_R,				
            REGEXP_REPLACE(A.DS_REGRA , '[^a-zA-Z0-9 ]', '') AS DS_REGRA,
            REGEXP_REPLACE(B.DS_OBSERVACAO , '[^a-zA-Z0-9 ]', '') AS DS_OBSERVACAO_CRITERIO_R,
            REGEXP_REPLACE(NVL((IE_LIB_LAUDO_PROC),'N') , '[^a-zA-Z0-9 ]', '') AS IE_LIB_LAUDO_PROC_R,
            B.DT_VIGENCIA_INICIAL                   DT_VIGENCIA_INICIAL_R,
            B.DT_VIGENCIA_FINAL                     DT_VIGENCIA_FINAL_R,
            B.DT_ATUALIZACAO                        DT_ATUALIZACAO_R
         FROM 	TASY.PROC_CRITERIO_REPASSE B,
                TASY.REGRA_REPASSE_TERCEIRO A
         WHERE
                A.CD_REGRA	= B.CD_REGRA
    """

def getFormaRepasse():
    return """
    SELECT
        RRTI.CD_REGRA               CD_REGRA_FR,
        RRTI.NR_SEQ_ITEM            NR_SEQ_ITEM_FR,
        RRTI.NR_SEQ_CATEGORIA       NR_SEQ_CATEGORIA_FR,
        RRTI.CD_PESSOA_FISICA       CD_PESSOA_FISICA_FR,
        RRTI.TX_REPASSE             TX_REPASSE_FR,
        RRTI.NR_SEQ_TERCEIRO        NR_SEQ_TERCEIRO_FR,
        RRTI.IE_BENEFICIARIO        IE_BENEFICIARIO_FR,
        RRTI.IE_FUNCAO_MEDICO       IE_FUNCAO_MEDICO_FR,
        RRTI.IE_PERC_SALDO          IE_PERC_SALDO_FR,
        RRTI.DT_FIM_VIGENCIA        DT_FIM_VIGENCIA_FR,
        RRTI.DT_ATUALIZACAO         DT_ATUALIZACAO_FR,
        CASE WHEN RRTI.IE_BENEFICIARIO = 'E' THEN 'EXECUTOR'
             WHEN RRTI.IE_BENEFICIARIO = 'T' THEN 'TERCEIRO'
             ELSE 'EXECUTOR' END EXECUTOR_TERCEIRO_FR,
        CASE WHEN RRTI.DT_FIM_VIGENCIA  < SYSDATE THEN 'INATIVO'
             ELSE 'ATIVO' END IE_SITUACAO_FR
        FROM TASY.REGRA_REPASSE_TERC_ITEM RRTI
    """

def getGlosa():
    return """
     WITH TBL_GLOSA AS (
            SELECT
                     1                              AS ID_ORIGEM_GLOSA,
                    'CONVENIO'                      AS ORIGEM_GLOSA,
                     crg.nr_seq_propaci             AS NR_SEQ_PROCED_GLOSA ,
                     nvl(cri.nr_interno_conta,0)    AS NR_INTERNO_CONTA_GLOSA,
                     nvl(crg.nr_seq_partic,0)       AS NR_SEQ_PARTIC_GLOSA,
                     'S'                            AS IE_GLOSA,
                     crg.vl_cobrado                 AS VL_COBRADO,
                     crg.vl_glosa                   AS VL_GLOSA,
                     --NULL                           AS DS_TIPO_AJUSTE_GLOSA,
                     CASE 
                         WHEN  crg.vl_cobrado < crg.vl_glosa THEN 'Inconsistente'
                         WHEN  crg.vl_cobrado = crg.vl_glosa THEN 'Glosa Total'
                         WHEN  crg.vl_cobrado <> crg.vl_glosa THEN 'Glosa Parcial'
                         END                              AS DS_TIPO_AJUSTE_GLOSA,
                     cr.dt_retorno                  AS DT_REF_GLOSA,
                     'E'                            AS IE_TIPO_AUDITORIA,
                     'Glosa Administrativa'         AS DS_TIPO_AUDITORIA,
                     NULL                           AS IE_TIPO_AUDITORIA_ITEM,
                     substr(TASY.obter_descricao_padrao('MOTIVO_GLOSA','DS_MOTIVO_GLOSA',crg.cd_motivo_glosa),1,255) AS DS_MOTIVO_GLOSA
           FROM tasy.convenio_retorno cr
           JOIN tasy.convenio_retorno_item cri on cr.nr_sequencia = cri.nr_seq_retorno
           JOIN tasy.convenio_retorno_glosa crg on cri.nr_sequencia = crg.nr_seq_ret_item and crg.cd_procedimento is not null
           JOIN (SELECT crg.nr_seq_propaci NR_SEQ_PROCED_GLOSA, 
                        max(cr.dt_retorno) DT_REF_GLOSA
                 FROM tasy.convenio_retorno cr
                            JOIN tasy.convenio_retorno_item cri on cr.nr_sequencia = cri.nr_seq_retorno
                            JOIN tasy.convenio_retorno_glosa crg on cri.nr_sequencia = crg.nr_seq_ret_item and crg.cd_procedimento is not null
                 WHERE cr.nr_seq_ret_estorno is null
                   and cr.ie_status_retorno = 'F'
                   and not exists (select 1 from tasy.convenio_retorno x where x.nr_seq_ret_estorno = cr.nr_sequencia)
                   and cr.dt_retorno >= TO_DATE('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                   and crg.vl_glosa > 0
                GROUP BY 
                crg.nr_seq_propaci)t  on t.NR_SEQ_PROCED_GLOSA = crg.nr_seq_propaci AND t.DT_REF_GLOSA =  cr.dt_retorno
            WHERE cr.nr_seq_ret_estorno is null
              and cr.ie_status_retorno = 'F'
              and not exists (select 1 from tasy.convenio_retorno x where x.nr_seq_ret_estorno = cr.nr_sequencia)
              --and cr.dt_retorno >= TO_DATE('2025-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
              AND cr.dt_retorno BETWEEN TO_DATE(:dt_inicio_carga, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dt_fim_carga, 'YYYY-MM-DD HH24:MI:SS')
              and crg.vl_glosa > 0
            
            UNION ALL
            
            SELECT
                2                                     AS ID_ORIGEM_GLOSA,
                'AUDITORIA'                           AS ORIGEM_GLOSA,
                b.nr_seq_propaci                      AS NR_SEQ_PROCED_GLOSA,
                NULL                                  AS NR_INTERNO_CONTA_GLOSA   ,
                0                                     AS NR_SEQ_PARTIC_GLOSA,
                'S'                                   AS IE_GLOSA,
                b.vl_procedimento                     AS VL_COBRADO,
                --b.vl_total_ajuste                     AS VL_AJUSTADO,
                CASE 
                     WHEN  b.VL_TOTAL_AJUSTE = 0 THEN (CASE WHEN b.VL_PROCEDIMENTO < 0 THEN b.VL_TOTAL_AJUSTE ELSE b.VL_PROCEDIMENTO END) -- Glosa Total. no entanto se o valor do procedimento for negativo mostra o vl_procedimento senão vl_ajuste
                     WHEN  b.VL_TOTAL_AJUSTE = b.VL_PROCEDIMENTO THEN 0 -- Não teve glosa
                     WHEN  b.VL_TOTAL_AJUSTE > b.VL_PROCEDIMENTO THEN null -- Ajuste de conta em auditoria externa. Com orientação de Dani não vamos considerar "ajuste de conta" como glosa. Regra definida em 11-07-2025
                     WHEN  b.VL_TOTAL_AJUSTE <> b.vl_procedimento THEN (b.VL_PROCEDIMENTO - b.VL_TOTAL_AJUSTE) -- Glosa Parcial. Faz a subtração
                     END                              AS VL_GLOSA,
                CASE 
                     WHEN  b.VL_TOTAL_AJUSTE = 0 THEN 'Glosa Total'
                     --WHEN  b.VL_PROCEDIMENTO < 0 THEN 'Ajuste de Conta'
                     WHEN  b.VL_TOTAL_AJUSTE > b.VL_PROCEDIMENTO THEN 'Ajuste de Conta'
                     WHEN  b.VL_TOTAL_AJUSTE = b.VL_PROCEDIMENTO THEN 'Inconsistente'
                     WHEN  b.VL_TOTAL_AJUSTE <> b.vl_procedimento THEN 'Glosa Parcial'
                     END                              AS DS_TIPO_AJUSTE_GLOSA,
                a.DT_AUDITORIA                        AS DT_REF_GLOSA,
                a.IE_TIPO_AUDITORIA                   AS IE_TIPO_AUDITORIA ,
                case when a.IE_TIPO_AUDITORIA = 'I' THEN 'Interna'
                     when a.IE_TIPO_AUDITORIA = 'E' THEN 'Externa' end DS_TIPO_AUDITORIA,
--                case when a.IE_TIPO_AUDITORIA = 'I' THEN 'Ajuste de conta'
--                     when a.IE_TIPO_AUDITORIA = 'E' THEN 'Glosa Técnica' end DS_CLASS_TIPO_AUDITORIA,     
                b.IE_TIPO_AUDITORIA                   AS IE_TIPO_AUDITORIA_ITEM,
--                case when b.IE_TIPO_AUDITORIA = 'V' THEN 'Atualização'
--                     when b.IE_TIPO_AUDITORIA = 'X' THEN 'Exclusão' end DS_IE_TIPO_AUDITORIA_ITEM,
                c.ds_motivo_auditoria                 AS DS_MOTIVO_GLOSA
            FROM tasy.auditoria_propaci b,
                 tasy.auditoria_conta_paciente a,
                 tasy.auditoria_motivo c
            where a.nr_sequencia = b.nr_seq_auditoria
            and c.nr_sequencia = b.nr_seq_motivo
            and b.nr_seq_motivo is not null
            and a.nr_sequencia = (select max(x.nr_seq_auditoria) from tasy.auditoria_propaci x where x.nr_seq_propaci = b.nr_seq_propaci)
            --and a.DT_AUDITORIA >=  TO_DATE('2025-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            and a.IE_TIPO_AUDITORIA =  'E' -- Auditoria Externa
            and b.IE_TIPO_AUDITORIA in ('V', 'X') -- V: auditoria do tipo "Alteração valor"; X: Auditoria do tipo exclusão de item
            AND a.DT_AUDITORIA BETWEEN TO_DATE(:dt_inicio_carga, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dt_fim_carga, 'YYYY-MM-DD HH24:MI:SS')

            UNION ALL
    
            SELECT
                3                                       AS ID_ORIGEM_GLOSA,
                'REPASSE'                               AS ORIGEM_GLOSA,
                pr.nr_seq_procedimento                  AS NR_SEQ_PROCED_GLOSA,
                PR.NR_INTERNO_CONTA                     AS NR_INTERNO_CONTA_GLOSA,
                0                                       AS NR_SEQ_PARTIC_GLOSA,
                'S'                                     AS IE_GLOSA,
                NULL                                    AS VL_COBRADO,
                NULL                                    AS VL_GLOSA,
                NULL                                    AS DS_TIPO_AJUSTE_GLOSA,
                DT_PROCEDIMENTO                         AS DT_REF_GLOSA,
                'I'                                     AS IE_TIPO_AUDITORIA,
                'Interna'                               AS DS_TIPO_AUDITORIA,
                NULL                                    AS IE_TIPO_AUDITORIA_ITEM,
                substr(tasy.obter_descricao_padrao('TIPO_REPASSE_ITEM','DS_TIPO',nr_seq_tipo),1,255) AS DS_MOTIVO_GLOSA
            FROM tasy.procedimento_repasse pr,
                 tasy.repasse_terceiro_item ri
            where pr.nr_repasse_terceiro = ri.nr_repasse_terceiro
            and nr_seq_tipo is not null
            and nr_seq_tipo not in (15, 16, 31)
            --and DT_PROCEDIMENTO >= TO_DATE('2025-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            AND DT_PROCEDIMENTO BETWEEN TO_DATE(:dt_inicio_carga, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dt_fim_carga, 'YYYY-MM-DD HH24:MI:SS')
            and pr.nr_repasse_terceiro = (select max(x.nr_repasse_terceiro) from tasy.procedimento_repasse x where x.nr_seq_procedimento = pr.nr_seq_procedimento)
            and ri.nr_sequencia_item = (select max(x.nr_sequencia_item) from TASY.repasse_terceiro_item x where x.nr_repasse_terceiro = pr.nr_repasse_terceiro)
            
            )
    
        SELECT 
        g.*
        FROM TBL_GLOSA g


    """

def getGlosa_old1():
    return """
    WITH TBL_GLOSA AS (
            SELECT
                     1 AS ID_ORIGEM_GLOSA,
                    'CONVENIO' ORIGEM_GLOSA,
                     crg.nr_seq_propaci NR_SEQ_PROCED_GLOSA ,
                     nvl(cri.nr_interno_conta,0) as NR_INTERNO_CONTA_GLOSA,
                     nvl(crg.nr_seq_partic,0) NR_SEQ_PARTIC_GLOSA,
                     substr(TASY.obter_descricao_padrao('MOTIVO_GLOSA','DS_MOTIVO_GLOSA',crg.cd_motivo_glosa),1,255)DS_MOTIVO_GLOSA,
                     'S' IE_GLOSA,
                     crg.vl_glosa VL_GLOSA,
                     crg.vl_cobrado VL_COBRADO,
                     cr.dt_retorno DT_REF_GLOSA,
                     null as IE_TIPO_AUDITORIA
           FROM tasy.convenio_retorno cr
           JOIN tasy.convenio_retorno_item cri on cr.nr_sequencia = cri.nr_seq_retorno
           JOIN tasy.convenio_retorno_glosa crg on cri.nr_sequencia = crg.nr_seq_ret_item and crg.cd_procedimento is not null
           JOIN (select crg.nr_seq_propaci NR_SEQ_PROCED_GLOSA,
                            max(cr.dt_retorno) DT_REF_GLOSA
                 FROM tasy.convenio_retorno cr
                            join tasy.convenio_retorno_item cri on cr.nr_sequencia = cri.nr_seq_retorno
                            join tasy.convenio_retorno_glosa crg on cri.nr_sequencia = crg.nr_seq_ret_item and crg.cd_procedimento is not null
                 WHERE cr.nr_seq_ret_estorno is null
                            and cr.ie_status_retorno = 'F'
                            and not exists (select 1 from tasy.convenio_retorno x where x.nr_seq_ret_estorno = cr.nr_sequencia)
                            and cr.dt_retorno >= TO_DATE('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                            and crg.vl_glosa > 0
                      group by 
                      crg.nr_seq_propaci)t  on t.NR_SEQ_PROCED_GLOSA = crg.nr_seq_propaci AND t.DT_REF_GLOSA =  cr.dt_retorno
            where cr.nr_seq_ret_estorno is null
              and cr.ie_status_retorno = 'F'
              and not exists (select 1 from tasy.convenio_retorno x where x.nr_seq_ret_estorno = cr.nr_sequencia)
              --and cr.dt_retorno >= TO_DATE(:dt_ref_glosa, 'YYYY-MM-DD HH24:MI:SS')
              AND cr.dt_retorno BETWEEN TO_DATE(:dt_inicio_carga, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dt_fim_carga, 'YYYY-MM-DD HH24:MI:SS')
              and crg.vl_glosa > 0
    
    
            
            UNION ALL
    
            SELECT
                2 AS ID_ORIGEM_GLOSA,
                'AUDITORIA' ORIGEM_GLOSA,
                b.nr_seq_propaci NR_SEQ_PROCED_GLOSA,
                NULL AS NR_INTERNO_CONTA_GLOSA   ,
                0 AS NR_SEQ_PARTIC_GLOSA,
                c.ds_motivo_auditoria as DS_MOTIVO_GLOSA,
                'S' IE_GLOSA,
                NULL VL_GLOSA,
                NULL VL_COBRADO,
                a.DT_AUDITORIA AS DT_REF_GLOSA,
                a.IE_TIPO_AUDITORIA
            FROM tasy.auditoria_propaci b,
                 tasy.auditoria_conta_paciente a,
                 tasy.auditoria_motivo c
            where a.nr_sequencia = b.nr_seq_auditoria
            and c.nr_sequencia = b.nr_seq_motivo
            and b.nr_seq_motivo is not null
            and a.nr_sequencia = (select max(x.nr_seq_auditoria) from tasy.auditoria_propaci x where x.nr_seq_propaci = b.nr_seq_propaci)
            --and DT_AUDITORIA >=  TO_DATE('2025-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            AND a.DT_AUDITORIA BETWEEN TO_DATE(:dt_inicio_carga, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dt_fim_carga, 'YYYY-MM-DD HH24:MI:SS')

            UNION ALL
    
            SELECT
                3 AS ID_ORIGEM_GLOSA,
                'REPASSE' ORIGEM_GLOSA,
                pr.nr_seq_procedimento NR_SEQ_PROCED_GLOSA,
                NULL AS NR_INTERNO_CONTA_GLOSA,
                0 AS NR_SEQ_PARTIC_GLOSA,
                substr(tasy.obter_descricao_padrao('TIPO_REPASSE_ITEM','DS_TIPO',nr_seq_tipo),1,255) DS_MOTIVO_GLOSA,
                'S' IE_GLOSA,
                NULL VL_GLOSA,
                NULL VL_COBRADO,
                DT_PROCEDIMENTO AS DT_REF_GLOSA,
                null as IE_TIPO_AUDITORIA 
            FROM tasy.procedimento_repasse pr,
                 tasy.repasse_terceiro_item ri
            where pr.nr_repasse_terceiro = ri.nr_repasse_terceiro
            and nr_seq_tipo is not null
            and nr_seq_tipo not in (15, 16, 31)
            --and DT_PROCEDIMENTO >= TO_DATE(:dt_ref_glosa, 'YYYY-MM-DD HH24:MI:SS')
            AND DT_PROCEDIMENTO BETWEEN TO_DATE(:dt_inicio_carga, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dt_fim_carga, 'YYYY-MM-DD HH24:MI:SS')
            and pr.nr_repasse_terceiro = (select max(x.nr_repasse_terceiro) from tasy.procedimento_repasse x where x.nr_seq_procedimento = pr.nr_seq_procedimento)
            and ri.nr_sequencia_item = (select max(x.nr_sequencia_item) from TASY.repasse_terceiro_item x where x.nr_repasse_terceiro = pr.nr_repasse_terceiro))
    
        SELECT 
        g.*
        , case when IE_TIPO_AUDITORIA = 'I' THEN 'Ajuste de conta'
               when IE_TIPO_AUDITORIA = 'E' THEN 'Glosa Técnica' 
               else null end DS_TIPO_AUDITORIA
        FROM TBL_GLOSA g
    """

def getGlosa_old():
    return """
    WITH TBL_GLOSA AS (SELECT
        1 AS ID_ORIGEM_GLOSA,
        'CONVENIO' ORIGEM_GLOSA,
        w.nr_sequencia NR_SEQ_PROCED_GLOSA,
        w.nr_interno_conta NR_INTERNO_CONTA_GLOSA,
        substr(TASY.obter_desc_motivo_glosa(y.cd_motivo_glosa),1,255) DS_MOTIVO_GLOSA,
        'S' IE_GLOSA,
        CASE WHEN (CASE WHEN vl_glosa = 0 AND  vl_glosa_informada = 0 THEN 100
                        WHEN round((vl_glosa*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) >= 100 OR
                             round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) >= 100 THEN 1
                        WHEN round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) > 0 AND
                             round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) < 100 THEN
                             round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2)
                        ELSE round((vl_glosa*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2)  END) = 0 THEN NULL
             ELSE (CASE WHEN round((vl_glosa*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) >= 100 OR
                             round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) >= 100 THEN 1
                        WHEN round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) > 0 AND
                             round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) < 100 THEN
                             round((vl_glosa_informada*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2)
                        ELSE round((vl_glosa*100/(CASE WHEN to_number(TASY.obter_dados_lote_audit_item(y.nr_sequencia,'VO')) = 0 THEN NULL END)),2) END )
        END AS PERC_GLOSA,
        DT_HISTORICO AS DT_REF_GLOSA
        FROM TASY.procedimento_paciente w,
             TASY.lote_audit_hist_item y
        where y.nr_seq_propaci	= w.nr_sequencia
            and dt_historico >=  TO_DATE(:dt_ref_glosa, 'YYYY-MM-DD HH24:MI:SS')
            and y.nr_sequencia = (select max(x.nr_sequencia) from TASY.lote_audit_hist_item x where x.nr_seq_propaci = w.nr_sequencia)

        UNION ALL

        SELECT
            2 AS ID_ORIGEM_GLOSA,
            'AUDITORIA' ORIGEM_GLOSA,
            b.nr_seq_propaci NR_SEQ_PROCED_GLOSA,
            NULL AS NR_INTERNO_CONTA_GLOSA   ,
            c.ds_motivo_auditoria as DS_MOTIVO_GLOSA,
            'S' IE_GLOSA,
            NULL PERC_GLOSA,
            a.DT_AUDITORIA AS DT_REF_GLOSA
        FROM tasy.auditoria_propaci b,
             tasy.auditoria_conta_paciente a,
             tasy.auditoria_motivo c
        where a.nr_sequencia = b.nr_seq_auditoria
        and c.nr_sequencia = b.nr_seq_motivo
        and b.nr_seq_motivo is not null
        and a.nr_sequencia = (select max(x.nr_seq_auditoria) from tasy.auditoria_propaci x where x.nr_seq_propaci = b.nr_seq_propaci)
        and a.DT_AUDITORIA >= TO_DATE(:dt_ref_glosa, 'YYYY-MM-DD HH24:MI:SS')

        UNION ALL

        SELECT
            3 AS ID_ORIGEM_GLOSA,
            'REPASSE' ORIGEM_GLOSA,
            pr.nr_seq_procedimento NR_SEQ_PROCED_GLOSA,
            NULL AS NR_INTERNO_CONTA_GLOSA,
            substr(tasy.obter_descricao_padrao('TIPO_REPASSE_ITEM','DS_TIPO',nr_seq_tipo),1,255) DS_MOTIVO_GLOSA,
            'S' IE_GLOSA,
            NULL PERC_GLOSA,
            DT_PROCEDIMENTO AS DT_REF_GLOSA
        FROM tasy.procedimento_repasse pr,
             tasy.repasse_terceiro_item ri
        where pr.nr_repasse_terceiro = ri.nr_repasse_terceiro
        and nr_seq_tipo is not null
        and nr_seq_tipo not in (15, 16, 31)
        and DT_PROCEDIMENTO >= TO_DATE(:dt_ref_glosa, 'YYYY-MM-DD HH24:MI:SS')
        and pr.nr_repasse_terceiro = (select max(x.nr_repasse_terceiro) from tasy.procedimento_repasse x where x.nr_seq_procedimento = pr.nr_seq_procedimento)
        and ri.nr_sequencia_item = (select max(x.nr_sequencia_item) from TASY.repasse_terceiro_item x where x.nr_repasse_terceiro = pr.nr_repasse_terceiro))

    SELECT 
    DISTINCT
    g.*
    FROM TBL_GLOSA g
    """

def getDadosMedico():
    return """
    SELECT
        DISTINCT
        pf.CD_PESSOA_FISICA,
        pf.NM_PESSOA_FISICA AS NM_MEDICO,
        pf.NR_CPF
    FROM TASY.PESSOA_FISICA PF
        INNER JOIN TASY.MEDICO M ON M.CD_PESSOA_FISICA = PF.CD_PESSOA_FISICA
        LEFT JOIN tasy.CONSELHO_PROFISSIONAL cp ON cp.NR_SEQUENCIA = pf.NR_SEQ_CONSELHO
    """

def getCDMedico(list_cpf_medicos):
    return f"""
    SELECT
        DISTINCT
        pf.CD_PESSOA_FISICA,
         pf.NM_PESSOA_FISICA AS NM_MEDICO,
        pf.NR_CPF
    FROM TASY.PESSOA_FISICA PF
        INNER JOIN TASY.MEDICO M ON M.CD_PESSOA_FISICA = PF.CD_PESSOA_FISICA
        LEFT JOIN tasy.CONSELHO_PROFISSIONAL cp ON cp.NR_SEQUENCIA = pf.NR_SEQ_CONSELHO
    where pf.NR_CPF in {list_cpf_medicos}
    """

def getAllProdMedica(dt_inicio_carga, dt_fim_carga, list_cd_pessoa_fisica_medicos):
    return f"""
    WITH PROD_MED AS
        (SELECT 
            PP.NR_SEQUENCIA 																													AS NR_SEQ_PROCED,
            PR.NR_SEQUENCIA 																													AS NR_SEQ_PROCED_REPASSE,
            0 																																    AS NR_SEQ_PARTIC,
            PP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PP,
            CP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_CP,
            PR.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PR,
            rnf.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_RNF,
            NULL																																AS DT_ATUALIZACAO_PPART,
            RT.DT_ATUALIZACAO                                                                                                                   AS DT_ATUALIZACAO_RT,
            TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA)																								AS CD_TAXA,
            TASY.OBTER_DESC_ESTRUT_CONTA(TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA))																AS DS_TAXA,
            PP.CD_SETOR_ATENDIMENTO 																											AS CD_SETOR_ATENDIMENTO,
            (SELECT DS_SETOR_ATENDIMENTO FROM TASY.SETOR_ATENDIMENTO SA WHERE PP.CD_SETOR_ATENDIMENTO = SA.CD_SETOR_ATENDIMENTO)                AS DS_SETOR_ATENDIMENTO,
            PP.CD_PROCEDIMENTO 																													AS CD_PROCEDIMENTO,
            PP.IE_ORIGEM_PROCED 																												AS IE_ORIGEM_PROCED,
            P.IE_CLASSIFICACAO																													AS IE_CLASSIFICACAO,
            PP.NR_PRESCRICAO 																													AS NR_PRESCRICAO,
            PP.NR_CIRURGIA 																														AS NR_CIRURGIA,
            PP.NR_SEQ_PROC_PACOTE 																												AS NR_SEQ_PROC_PACOTE,
            PP.NR_ATENDIMENTO 																													AS NR_ATENDIMENTO,
            AP.CD_MEDICO_RESP 																													AS CD_MEDICO_RESP,
            PP.DT_CONTA																															AS DT_CONTA,
            PP.DT_PROCEDIMENTO 																													AS DT_PROCEDIMENTO,
            TASY.OBTER_MEDICO_LAUDO_SEQUENCIA(PP.NR_LAUDO,'C') 																					AS CD_MEDICO_LAUDO,
            PP.NR_INTERNO_CONTA 																												AS NR_INTERNO_CONTA,
            PP.NR_SEQ_PROC_INTERNO 																												AS NR_SEQ_PROC_INTERNO,
            AP.DT_ALTA 																															AS DT_ALTA,
            CP.DT_MESANO_REFERENCIA 																											AS DT_MESANO_REFERENCIA,
            AP.DT_ENTRADA 																														AS DT_ENTRADA,
            CP.CD_CONVENIO_PARAMETRO 																											AS CD_CONVENIO_PARAMETRO,
            (SELECT C.DS_CONVENIO FROM tasy.CONVENIO C WHERE C.CD_CONVENIO = CP.CD_CONVENIO_PARAMETRO)                                          AS DS_CONVENIO,
            nvl(round(TASY.OBTER_EDICAO(CP.CD_ESTABELECIMENTO, CP.CD_CONVENIO_PARAMETRO, CP.CD_CATEGORIA_PARAMETRO, AP.DT_ENTRADA, NULL),0), 0)	AS CD_EDICAO_AMB,
            CP.CD_ESTABELECIMENTO 																												AS CD_ESTABELECIMENTO,
            (SELECT NM_FANTASIA_ESTAB FROM TASY.ESTABELECIMENTO E WHERE E.CD_ESTABELECIMENTO = CP.CD_ESTABELECIMENTO)                           AS DS_ESTABELECIMENTO,
            -- PP.CD_MEDICO_EXECUTOR 																										    AS CD_MEDICO_EXECUTOR,
            NVL(PR.CD_MEDICO, PP.CD_MEDICO_EXECUTOR)                                                                                            AS CD_MEDICO_EXECUTOR,
            PR.CD_MEDICO																														AS CD_MEDICO_REPASSE,
            PP.CD_CGC_PRESTADOR 																												AS CD_CGC_PRESTADOR,
            PP.IE_FUNCAO_MEDICO 																												AS IE_FUNCAO_MEDICO,
            'A' 																																AS IE_PARTICIPOU_SUS,
            PP.IE_RESPONSAVEL_CREDITO 																											AS IE_RESPONSAVEL_CREDITO,
            AP.IE_TIPO_ATENDIMENTO 																												AS IE_TIPO_ATENDIMENTO,
            nvl(PP.IE_TIPO_ATO_SUS,0)				 																							AS IE_TIPO_ATO_SUS,
            nvl(PP.IE_TIPO_SERVICO_SUS,0)																										AS IE_TIPO_SERVICO_SUS,
            CP.NM_USUARIO_ORIGINAL 																												AS NM_USUARIO_ORIGINAL,
            nvl(TASY.OBTER_ETAPA_CHECKUP_PROC(PP.NR_SEQUENCIA),0) 																				AS NR_SEQ_ETAPA_CHECKUP,
            pr.VL_LIBERADO 																														AS VL_LIBERADO,
            PP.QT_PROCEDIMENTO 																													AS QT_PROCEDIMENTO,
            PP.VL_PROCEDIMENTO																													AS VL_PROCEDIMENTO,
            0 																																    AS VL_PARTICIPANTE,
            PP.VL_MEDICO 																														AS VL_MEDICO,
            PP.VL_ANESTESISTA 																													AS VL_ANESTESISTA,
            PP.VL_MATERIAIS 																													AS VL_MATERIAIS,
            PP.VL_AUXILIARES 																													AS VL_AUXILIARES,
            PP.VL_CUSTO_OPERACIONAL 																											AS VL_CUSTO_OPERACIONAL,
            tasy.obter_desc_prescr_proc_exam(pp.cd_procedimento, pp.ie_origem_proced, pp.NR_SEQ_PROC_INTERNO, pp.NR_SEQ_EXAME ) 				AS DS_PROC_INTERNO,
            tasy.obter_nome_pf_pj(NVL(PR.CD_MEDICO, PP.CD_MEDICO_EXECUTOR), null) 			 													AS NM_MEDICO_EXEC,
            TASY.OBTER_DESCRICAO_PADRAO('FUNCAO_MEDICO', 'DS_FUNCAO', PP.IE_FUNCAO_MEDICO) 														AS DS_FUNCAO_MEDICO,
            EP.CD_ESPECIALIDADE                                                                                                                 AS CD_ESPECIALIDADE,
            EP.DS_ESPECIALIDADE 																												AS DS_ESPECIALIDADE,
            cp.NR_PROTOCOLO 																													AS NR_PROTOCOLO,
            cp.NR_SEQ_PROTOCOLO 																												AS NR_SEQ_PROTOCOLO,
            TASY.OBTER_STATUS_PROTOCOLO(cp.NR_SEQ_PROTOCOLO) 																					AS IE_STATUS_PROTOCOLO,
            pc.dt_definitivo                                                                                                                    AS DT_DEFINITIVO,
            cp.IE_STATUS_ACERTO 																												AS IE_STATUS_ACERTO,
            tasy.obter_pessoa_atendimento(pp.nr_atendimento, 'N') 																				AS NM_PACIENTE,
            pp.CD_REGRA_REPASSE 																												AS CD_REGRA_REPASSE,
            pp.NR_SEQ_PROC_CRIT_REPASSE 																										AS NR_SEQ_CRITERIO_REPASSE,
            pr.nr_repasse_terceiro 																												AS NR_REPASSE_TERCEIRO,
            tasy.obter_nf_repasse(pr.nr_repasse_terceiro)  																						AS NR_NOTA_FISCAL,
            rnf.NR_SEQ_NOTA_FISCAL 																												AS NR_SEQ_NOTA_FISCAL,
            rt.DT_APROVACAO_TERCEIRO 																											AS DT_APROVACAO_TERCEIRO,
            TASY.OBTER_VALOR_CONTA(CP.NR_INTERNO_CONTA,0) 																						AS VL_CONTA,
            pr.vl_repasse 																														AS VL_REPASSE,
            TASY.OBTER_VALOR_ITEM_AUDIT(PP.NR_INTERNO_CONTA, PP.NR_SEQUENCIA, NULL, 'Q') 														AS ITEM_AUDIT,
            PP.CD_MOTIVO_EXC_CONTA 																												AS CD_MOTIVO_EXC_CONTA,
            p.cd_tipo_procedimento  																											AS CD_TIPO_PROCEDIMENTO,
            TASY.OBTER_VALOR_DOMINIO(95, p.CD_TIPO_PROCEDIMENTO)                                                                                AS DS_TIPO_PROCEDIMENTO,
            P.ds_procedimento 																													AS DS_PROCEDIMENTO,
            P.CD_GRUPO_PROC																														AS CD_GRUPO_PROC,
            tasy.obter_tipo_acomodacao(ap.nr_atendimento) 																						AS CD_TIPO_ACOMODACAO,
            tasy.Obter_se_atend_retorno(ap.nr_atendimento) 																						AS IE_ATENDIMENTO_RETORNO,
            mp.DS_MOTIVO_EXC_CONTA 																												AS DS_MOTIVO_EXC_CONTA,
            CP.DT_PERIODO_INICIAL 																												AS DT_PERIODO_INICIAL,
            CP.DT_PERIODO_FINAL    																												AS DT_PERIODO_FINAL,
            PR.NR_SEQ_ORIGEM																													AS NR_SEQ_ORIGEM,
            rt.IE_STATUS																														AS IE_STATUS,
            rt.DT_ULT_ENVIO_EMAIL																												AS DT_ULT_ENVIO_EMAIL,
            (SELECT e.nr_sequencia
            from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
            WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
            and f.nr_sequencia = cpe.nr_seq_etapa
            and f.nr_seq_classif = e.nr_sequencia
            and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     and x.dt_fim_etapa is null)) 												AS CD_ETAPA ,
            (SELECT e.ds_classificacao
             from TASY.conta_paciente_etapa cpe,
                  tasy.fatur_etapa f,
                  tasy.classif_etapa e
             WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
             and f.nr_sequencia = cpe.nr_seq_etapa
             and f.nr_seq_classif = e.nr_sequencia
             and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia = (select max(x.nr_sequencia)
                                                                  from tasy.conta_paciente_etapa x
                                                                  where cpe.nr_interno_conta = x.nr_interno_conta
                                                                  and x.dt_fim_etapa is null)) 												  	AS DS_ETAPA,
            (SELECT min(cpe.dt_etapa)
            from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
            WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
            and f.nr_sequencia = cpe.nr_seq_etapa
            and f.nr_seq_classif = e.nr_sequencia
            and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     and x.dt_fim_etapa is null)) 												AS DT_ETAPA ,
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) 
             or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,pp.ie_funcao_medico) = pp.ie_funcao_medico
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'I') 																											AS REGRA_PACOTE_1,
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,pp.ie_funcao_medico) = pp.ie_funcao_medico
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'P')																											AS REGRA_PACOTE_2,
            (SELECT CE.nr_dias_venc_atend FROM TASY.convenio_estabelecimento CE 
             WHERE CP.CD_CONVENIO_PARAMETRO = CE.CD_CONVENIO AND cp.CD_ESTABELECIMENTO = CE.CD_ESTABELECIMENTO )                                AS NR_DIAS_VENC_ATEND,
            PP.CD_SITUACAO_GLOSA                                                                                                                AS CD_SITUACAO_GLOSA,
			(select 	nvl(max(IE_CONSISTE_SIT_GLOSA),'S') from TASY.parametro_faturamento where cd_estabelecimento =  AP.cd_estabelecimento)  AS IE_CONSISTE_SIT_GLOSA,
            ap.ie_clinica                                                                                                                       AS IE_CLINICA,
            (select max(v.dt_baixa_escritural) from tasy.dkz_pag_escritural_repasse_v v 
            where v.nr_repasse_terceiro = pr.nr_repasse_terceiro group by v.nr_repasse_terceiro)                                                AS DT_BAIXA_ESCRITURAL,
            CP.IE_CANCELAMENTO                                                                                                                  AS IE_CANCELAMENTO_CONTA,
            CASE WHEN PR.IE_STATUS = 'E' THEN 'E' ELSE 'N' END                                                                                  AS IE_STATUS_REPASSE,
            CP.DT_CONTA_PROTOCOLO                                                                                                               AS DT_CONTA_PROTOCOLO
        FROM
            TASY.conta_paciente CP,
            TASY.procedimento P,
            TASY.procedimento_repasse PR,
            TASY.procedimento_paciente PP,
            TASY.atendimento_paciente AP,
            TASY.ESPECIALIDADE_MEDICA EP,
            TASY.REPASSE_TERCEIRO rt,
            tasy.REPASSE_NOTA_FISCAL rnf,
            tasy.motivo_exc_conta_paciente mp,
            tasy.protocolo_convenio pc

        WHERE
            PP.nr_sequencia = PR.nr_seq_procedimento (+)
            AND PP.cd_procedimento = P.cd_procedimento AND PP.ie_origem_proced = p.ie_origem_proced
            AND PP.nr_atendimento = AP.nr_atendimento
            AND PP.nr_interno_conta = cp.nr_interno_conta
            AND pp.CD_ESPECIALIDADE = EP.CD_ESPECIALIDADE  (+)
            AND PR.NR_REPASSE_TERCEIRO = RT.NR_REPASSE_TERCEIRO (+)
            AND RT.NR_REPASSE_TERCEIRO = rnf.NR_REPASSE_TERCEIRO (+)
            AND pp.cd_motivo_exc_conta = mp.cd_motivo_exc_conta (+)
            AND cp.NR_SEQ_PROTOCOLO = pc.NR_SEQ_PROTOCOLO (+)
            AND PR.nr_seq_partic(+) IS NULL
            AND PP.CD_MEDICO_EXECUTOR IS NOT NULL
            AND pr.dt_liberacao(+) is not null -- Apenas procedimentos liberados, desconsidera "rascunhos"
           
        UNION ALL

        SELECT
            PPART.NR_SEQUENCIA 																													AS NR_SEQ_PROCED,
            PR.NR_SEQUENCIA 																													AS NR_SEQ_PROCED_REPASSE,
            CAST(PPART.NR_SEQ_PARTIC AS INT) 																									AS NR_SEQ_PARTIC,
            PP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PP,
            CP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_CP,
            PR.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PR,
            rnf.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_RNF,
            PPART.DT_ATUALIZACAO																												AS DT_ATUALIZACAO_PPART,
            RT.DT_ATUALIZACAO                                                                                                                   AS DT_ATUALIZACAO_RT,
            TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA)																								AS CD_TAXA,
            TASY.OBTER_DESC_ESTRUT_CONTA(TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA))																AS DS_TAXA,
            PP.CD_SETOR_ATENDIMENTO 																											AS CD_SETOR_ATENDIMENTO,
            (SELECT DS_SETOR_ATENDIMENTO FROM TASY.SETOR_ATENDIMENTO SA WHERE PP.CD_SETOR_ATENDIMENTO = SA.CD_SETOR_ATENDIMENTO)                AS DS_SETOR_ATENDIMENTO,
            PP.CD_PROCEDIMENTO 																													AS CD_PROCEDIMENTO,
            PP.IE_ORIGEM_PROCED 																												AS IE_ORIGEM_PROCED,
            P.IE_CLASSIFICACAO																													AS IE_CLASSIFICACAO,
            PP.NR_PRESCRICAO 																													AS NR_PRESCRICAO,
            PP.NR_CIRURGIA																														AS NR_CIRURGIA,
            PP.NR_SEQ_PROC_PACOTE 																												AS NR_SEQ_PROC_PACOTE,
            PP.NR_ATENDIMENTO 																													AS NR_ATENDIMENTO,
            AP.CD_MEDICO_RESP 																													AS CD_MEDICO_RESP,
            PP.DT_CONTA 																														AS DT_CONTA,
            PP.DT_PROCEDIMENTO 																													AS DT_PROCEDIMENTO,
            TASY.OBTER_MEDICO_LAUDO_SEQUENCIA(PP.NR_LAUDO,'C') 																					AS CD_MEDICO_LAUDO,
            PP.NR_INTERNO_CONTA 																												AS NR_INTERNO_CONTA,
            PP.NR_SEQ_PROC_INTERNO 																												AS NR_SEQ_PROC_INTERNO,
            AP.DT_ALTA 																															AS DT_ALTA,
            CP.DT_MESANO_REFERENCIA 																											AS DT_MESANO_REFERENCIA,
            AP.DT_ENTRADA 																														AS DT_ENTRADA,
            CP.CD_CONVENIO_PARAMETRO 																											AS CD_CONVENIO_PARAMETRO,
            (SELECT C.DS_CONVENIO FROM tasy.CONVENIO C WHERE C.CD_CONVENIO = CP.CD_CONVENIO_PARAMETRO)                                          AS DS_CONVENIO,
            nvl(round(TASY.OBTER_EDICAO(CP.CD_ESTABELECIMENTO, CP.CD_CONVENIO_PARAMETRO, CP.CD_CATEGORIA_PARAMETRO, AP.DT_ENTRADA, NULL),0),0)  AS CD_EDICAO_AMB ,
            CP.CD_ESTABELECIMENTO 																												AS CD_ESTABELECIMENTO,
            (SELECT NM_FANTASIA_ESTAB FROM TASY.ESTABELECIMENTO E WHERE E.CD_ESTABELECIMENTO = CP.CD_ESTABELECIMENTO)                           AS DS_ESTABELECIMENTO,
            PPART.CD_PESSOA_FISICA 																												AS CD_MEDICO_EXECUTOR,
            PR.CD_MEDICO																														AS CD_MEDICO_REPASSE,
            PPART.CD_CGC 																														AS CD_CGC_PRESTADOR,
            PPART.IE_FUNCAO 																													AS IE_FUNCAO_MEDICO,
            PPART.IE_PARTICIPOU_SUS 																											AS IE_PARTICIPOU_SUS,
            PPART.IE_RESPONSAVEL_CREDITO 																										AS IE_RESPONSAVEL_CREDITO,
            AP.IE_TIPO_ATENDIMENTO 																												AS IE_TIPO_ATENDIMENTO,
            nvl(PPART.IE_TIPO_ATO_SUS,0) 																										AS IE_TIPO_ATO_SUS,
            nvl(PPART.IE_TIPO_SERVICO_SUS,0)																									AS IE_TIPO_SERVICO_SUS,
            CP.NM_USUARIO_ORIGINAL 																												AS NM_USUARIO_ORIGINAL,
            nvl(TASY.OBTER_ETAPA_CHECKUP_PROC(PP.NR_SEQUENCIA),0) 																				AS NR_SEQ_ETAPA_CHECKUP,
            pr.VL_LIBERADO																														AS VL_LIBERADO,
            PP.QT_PROCEDIMENTO 																													AS QT_PROCEDIMENTO,
            PP.VL_PROCEDIMENTO																													AS VL_PROCEDIMENTO,
            nvl(PPART.VL_PARTICIPANTE,0)																										AS VL_PARTICIPANTE,
            0 																																    AS VL_MEDICO,
            PP.VL_ANESTESISTA 																													AS VL_ANESTESISTA,
            PP.VL_MATERIAIS 																													AS VL_MATERIAIS,
            PP.VL_AUXILIARES 																													AS VL_AUXILIARES,
            PP.VL_CUSTO_OPERACIONAL																												AS VL_CUSTO_OPERACIONAL,
            tasy.obter_desc_prescr_proc_exam(pp.cd_procedimento, pp.ie_origem_proced, pp.NR_SEQ_PROC_INTERNO, pp.NR_SEQ_EXAME ) 				AS DS_PROC_INTERNO,
            tasy.obter_nome_pessoa_fisica(PPART.CD_PESSOA_FISICA, null)														AS NM_MEDICO_EXEC, 
            TASY.OBTER_DESCRICAO_PADRAO('FUNCAO_MEDICO', 'DS_FUNCAO', PPART.IE_FUNCAO) 															AS DS_FUNCAO_MEDICO,
            EP.CD_ESPECIALIDADE                                                                                                                 AS CD_ESPECIALIDADE,
            EP.DS_ESPECIALIDADE 																												AS DS_ESPECIALIDADE,
            cp.NR_PROTOCOLO 																													AS NR_PROTOCOLO,
            cp.NR_SEQ_PROTOCOLO 																												AS NR_SEQ_PROTOCOLO,
            TASY.OBTER_STATUS_PROTOCOLO(cp.NR_SEQ_PROTOCOLO) 																					AS IE_STATUS_PROTOCOLO,
            pc.dt_definitivo                                                                                                                    AS DT_DEFINITIVO,
            cp.IE_STATUS_ACERTO 																												AS IE_STATUS_ACERTO,
            tasy.obter_pessoa_atendimento(pp.nr_atendimento, 'N') 																				AS NM_PACIENTE,
            ppart.CD_REGRA_REPASSE 																												AS CD_REGRA_REPASSE,
            ppart.NR_SEQ_PROC_CRIT_REPASSE 																										AS NR_SEQ_CRITERIO_REPASSE,
            pr.nr_repasse_terceiro 																												AS NR_REPASSE_TERCEIRO, 
            tasy.obter_nf_repasse(pr.nr_repasse_terceiro) 																						AS NR_NOTA_FISCAL,
            rnf.NR_SEQ_NOTA_FISCAL 																												AS NR_SEQ_NOTA_FISCAL,
            rt.DT_APROVACAO_TERCEIRO 																											AS DT_APROVACAO_TERCEIRO,
            TASY.OBTER_VALOR_CONTA(CP.NR_INTERNO_CONTA,0) 																						AS VL_CONTA,
            pr.vl_repasse 																														AS VL_REPASSE,	
            TASY.OBTER_VALOR_ITEM_AUDIT(PP.NR_INTERNO_CONTA, PP.NR_SEQUENCIA, NULL, 'Q') 														AS ITEM_AUDIT,
            PP.CD_MOTIVO_EXC_CONTA 																												AS CD_MOTIVO_EXC_CONTA,
            p.cd_tipo_procedimento 																												AS CD_TIPO_PROCEDIMENTO,
            TASY.OBTER_VALOR_DOMINIO(95, p.CD_TIPO_PROCEDIMENTO)                                                                                AS DS_TIPO_PROCEDIMENTO,
            P.ds_procedimento 																													AS DS_PROCEDIMENTO,
            P.CD_GRUPO_PROC																														AS CD_GRUPO_PROC,
            tasy.obter_tipo_acomodacao(ap.nr_atendimento) 																						AS CD_TIPO_ACOMODACAO,
            tasy.Obter_se_atend_retorno(ap.nr_atendimento) 																						AS IE_ATENDIMENTO_RETORNO,
            mp.DS_MOTIVO_EXC_CONTA 																												AS DS_MOTIVO_EXC_CONTA,
            CP.DT_PERIODO_INICIAL 																												AS DT_PERIODO_INICIAL,
            CP.DT_PERIODO_FINAL  																												AS DT_PERIODO_FINAL,
            PR.NR_SEQ_ORIGEM																													AS NR_SEQ_ORIGEM,
            rt.IE_STATUS 																														AS IE_STATUS,
            rt.DT_ULT_ENVIO_EMAIL																												AS DT_ULT_ENVIO_EMAIL,
            (SELECT e.nr_sequencia
                 from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
                 WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
                 AND f.nr_sequencia = cpe.nr_seq_etapa
                 AND f.nr_seq_classif = e.nr_sequencia
                 AND cpe.dt_fim_etapa is NULL AND cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     AND x.dt_fim_etapa is null)) 												AS CD_ETAPA,
            (SELECT e.ds_classificacao
                 from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
                 WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
                 AND f.nr_sequencia = cpe.nr_seq_etapa
                 AND f.nr_seq_classif = e.nr_sequencia
                 AND cpe.dt_fim_etapa is NULL AND cpe.nr_sequencia = (select max(x.nr_sequencia)
                                                                      from tasy.conta_paciente_etapa x
                                                                      where cpe.nr_interno_conta = x.nr_interno_conta
                                                                      AND x.dt_fim_etapa is null)) 												AS DS_ETAPA,
            (SELECT min(cpe.dt_etapa)
            from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
            WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
            and f.nr_sequencia = cpe.nr_seq_etapa
            and f.nr_seq_classif = e.nr_sequencia
            and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     and x.dt_fim_etapa is null)) 												AS DT_ETAPA,
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) 
             or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,ppart.ie_funcao) = ppart.ie_funcao
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'I')																											AS REGRA_PACOTE_1,		
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) 
             or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,ppart.ie_funcao) = ppart.ie_funcao
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'P')																											AS REGRA_PACOTE_2,
            (SELECT CE.nr_dias_venc_atend FROM TASY.convenio_estabelecimento CE 
             WHERE CP.CD_CONVENIO_PARAMETRO = CE.CD_CONVENIO AND cp.CD_ESTABELECIMENTO = CE.CD_ESTABELECIMENTO )                                AS NR_DIAS_VENC_ATEND,
             PP.CD_SITUACAO_GLOSA                                                                                                               AS CD_SITUACAO_GLOSA,
            (select nvl(max(IE_CONSISTE_SIT_GLOSA),'S') from TASY.parametro_faturamento where cd_estabelecimento =  AP.cd_estabelecimento)      AS IE_CONSISTE_SIT_GLOSA,
             ap.ie_clinica                                                                                                                      AS IE_CLINICA,
            (select max(v.dt_baixa_escritural) from tasy.dkz_pag_escritural_repasse_v v 
            where v.nr_repasse_terceiro = pr.nr_repasse_terceiro group by v.nr_repasse_terceiro)                                                AS DT_BAIXA_ESCRITURAL,
            CP.IE_CANCELAMENTO                                                                                                                  AS IE_CANCELAMENTO_CONTA,
            CASE WHEN PR.IE_STATUS = 'E' THEN 'E' ELSE 'N' END                                                                                  AS IE_STATUS_REPASSE,
            CP.DT_CONTA_PROTOCOLO                                                                                                               AS DT_CONTA_PROTOCOLO
        FROM
            TASY.conta_paciente CP,
            TASY.procedimento P,
            TASY.procedimento_participante PPART ,
            TASY.procedimento_repasse PR,
            TASY.procedimento_paciente PP,
            TASY.atendimento_paciente AP,
            TASY.ESPECIALIDADE_MEDICA EP,
            TASY.REPASSE_TERCEIRO rt,
            tasy.REPASSE_NOTA_FISCAL rnf,
            tasy.motivo_exc_conta_paciente mp,
            tasy.protocolo_convenio pc

        WHERE
            PP.nr_sequencia = PPART.NR_SEQUENCIA
            AND (PP.cd_procedimento = P.cd_procedimento AND PP.ie_origem_proced = P.ie_origem_proced)
            AND PP.nr_atendimento = AP.nr_atendimento
            AND PP.nr_interno_conta = CP.nr_interno_conta
            AND PR.nr_seq_procedimento(+) = PPART.nr_sequencia
            AND PR.nr_seq_partic(+) = PPART.nr_seq_partic
            AND PPART.CD_ESPECIALIDADE = EP.CD_ESPECIALIDADE  (+)
            AND PR.NR_REPASSE_TERCEIRO = RT.NR_REPASSE_TERCEIRO (+)
            AND RT.NR_REPASSE_TERCEIRO = rnf.NR_REPASSE_TERCEIRO (+)
            AND pp.cd_motivo_exc_conta = mp.cd_motivo_exc_conta (+)
            AND cp.NR_SEQ_PROTOCOLO = pc.NR_SEQ_PROTOCOLO (+)
            AND pr.dt_liberacao(+) is not null -- Apenas procedimentos liberados, desconsidera "rascunhos"
            AND PPART.CD_PESSOA_FISICA IS NOT NULL
        )

        SELECT 
        t.*,
        tab2.vl_estorno as VL_ESTORNO,
        case when tab2.vl_estorno is not null then  'S' else 'N' end IE_ESTORNO
        FROM PROD_MED t
        left join (select 
                       nr_sequencia as NR_SEQ_PROCED_REPASSE,
                       (select max(vl_liberado) from tasy.procedimento_repasse x 
                       where dt_liberacao is not null
                           and x.ie_status = 'E'
                           and x.nr_repasse_terceiro = a.nr_repasse_terceiro
                           and x.nr_seq_procedimento = a.nr_seq_procedimento
                           and a.nr_seq_partic = x.nr_seq_partic
                           and nvl(x.cd_medico,x.nr_seq_partic) = nvl(a.cd_medico,a.nr_seq_partic)
                       ) as  VL_ESTORNO
                   from tasy.procedimento_repasse a
                   where 1=1
                   --a.ie_status <> 'E'
                   and dt_liberacao is not null
                   -- VERIFICA SE EXISTE O PROCEDIMENTO DUPLICADO DENTRO DE UM MESMO REPASSE 
                   /*and nr_repasse_terceiro = (select min(x.nr_repasse_terceiro)
                                                   from tasy.procedimento_repasse x
                                              where x.nr_seq_procedimento = a.nr_seq_procedimento
                                                    and x.cd_medico = a.cd_medico
                                                    and x.dt_liberacao is not null
                                                    and nvl(x.cd_medico,x.nr_seq_partic) = nvl(a.cd_medico,a.nr_seq_partic))*/
                                                    )tab2 on tab2.NR_SEQ_PROCED_REPASSE = t.NR_SEQ_PROCED_REPASSE
        WHERE
        CD_TAXA NOT IN (72, 73,74) -- gasoterapia, diárias, taxa
        AND NVL(CD_TIPO_PROCEDIMENTO,0) NOT IN (85) --NVL pois tem valores nulos --(20 = Laboratório, 37 = Diária, 38 = Taxa,  85 = Fisioterapia, 133 = Curativo) 
        AND CD_SETOR_ATENDIMENTO != 30 -- Laboratório
        AND CD_ESPECIALIDADE NOT IN( 102, 101, 108) --Fisioterapia, Fonodiaulogia, Bioquimico
        AND IE_CANCELAMENTO_CONTA IS NULL -- APENAS CONTAS NÃO CANCELADAS
        AND CD_PROCEDIMENTO <> 10104020 --Atendimento medico do intensivista em UTI geral ou pediatrica (plantao de 12 horas - por paciente)
        AND IE_STATUS_REPASSE <> 'E' --Exclui itens estornados
        AND CD_MEDICO_EXECUTOR IN {list_cd_pessoa_fisica_medicos} -- Lista de médicos executores
        --AND CD_MEDICO_EXECUTOR IN (360, 642, 643, 191097, 231689, 257429, 433334, 231750, 229454, 231751, 636, 809, 595, 191097, 431198, 422, 810, 422187, 706, 
        --                           591,649,648,590,302381,254837,228923,231935,724,438517,532,231567,901,589,318376,592,447571,300260,394414,443521,275227,321493,441569,194772,233144,415015,199822,272957,549,652,239328,444757,424417,258586,659,231739,354274,228731,653,658,434209,395352,401046,368528,326954,572 )
        AND DT_PROCEDIMENTO BETWEEN TO_DATE('{dt_inicio_carga}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{dt_fim_carga}', 'YYYY-MM-DD HH24:MI:SS')
        -- AND DT_PROCEDIMENTO BETWEEN TO_DATE('2025-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('2025-07-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
        -- AND NR_ATENDIMENTO =  1198591
        -- AND  CD_ESPECIALIDADE IN (2,12,20,88,4) -- Lista de especialidades médicas
                                                        --    2  -- Ortopedia e Traumatologia
                                                        --    12 -- Oftalmologia
                                                        --    20 --Ginecologia e Obstetrícia
                                                        --    88 -- Ginecologia e obstetrícia (Inativo)
                                                        --    4  -- Cirurgia Geral
    """

def getAllProdMedica_incr(dt_inicio_carga, dt_fim_carga, list_cd_pessoa_fisica_medicos):
    return f"""
    WITH PROD_MED AS
        (SELECT 
            PP.NR_SEQUENCIA 																													AS NR_SEQ_PROCED,
            PR.NR_SEQUENCIA 																													AS NR_SEQ_PROCED_REPASSE,
            0 																																    AS NR_SEQ_PARTIC,
            PP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PP,
            CP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_CP,
            PR.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PR,
            rnf.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_RNF,
            NULL																																AS DT_ATUALIZACAO_PPART,
            RT.DT_ATUALIZACAO                                                                                                                   AS DT_ATUALIZACAO_RT,
            TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA)																								AS CD_TAXA,
            TASY.OBTER_DESC_ESTRUT_CONTA(TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA))																AS DS_TAXA,
            PP.CD_SETOR_ATENDIMENTO 																											AS CD_SETOR_ATENDIMENTO,
            (SELECT DS_SETOR_ATENDIMENTO FROM TASY.SETOR_ATENDIMENTO SA WHERE PP.CD_SETOR_ATENDIMENTO = SA.CD_SETOR_ATENDIMENTO)                AS DS_SETOR_ATENDIMENTO,
            PP.CD_PROCEDIMENTO 																													AS CD_PROCEDIMENTO,
            PP.IE_ORIGEM_PROCED 																												AS IE_ORIGEM_PROCED,
            P.IE_CLASSIFICACAO																													AS IE_CLASSIFICACAO,
            PP.NR_PRESCRICAO 																													AS NR_PRESCRICAO,
            PP.NR_CIRURGIA 																														AS NR_CIRURGIA,
            PP.NR_SEQ_PROC_PACOTE 																												AS NR_SEQ_PROC_PACOTE,
            PP.NR_ATENDIMENTO 																													AS NR_ATENDIMENTO,
            AP.CD_MEDICO_RESP 																													AS CD_MEDICO_RESP,
            PP.DT_CONTA																															AS DT_CONTA,
            PP.DT_PROCEDIMENTO 																													AS DT_PROCEDIMENTO,
            TASY.OBTER_MEDICO_LAUDO_SEQUENCIA(PP.NR_LAUDO,'C') 																					AS CD_MEDICO_LAUDO,
            PP.NR_INTERNO_CONTA 																												AS NR_INTERNO_CONTA,
            PP.NR_SEQ_PROC_INTERNO 																												AS NR_SEQ_PROC_INTERNO,
            AP.DT_ALTA 																															AS DT_ALTA,
            CP.DT_MESANO_REFERENCIA 																											AS DT_MESANO_REFERENCIA,
            AP.DT_ENTRADA 																														AS DT_ENTRADA,
            CP.CD_CONVENIO_PARAMETRO 																											AS CD_CONVENIO_PARAMETRO,
            (SELECT C.DS_CONVENIO FROM tasy.CONVENIO C WHERE C.CD_CONVENIO = CP.CD_CONVENIO_PARAMETRO)                                          AS DS_CONVENIO,
            nvl(round(TASY.OBTER_EDICAO(CP.CD_ESTABELECIMENTO, CP.CD_CONVENIO_PARAMETRO, CP.CD_CATEGORIA_PARAMETRO, AP.DT_ENTRADA, NULL),0), 0)	AS CD_EDICAO_AMB,
            CP.CD_ESTABELECIMENTO 																												AS CD_ESTABELECIMENTO,
            (SELECT NM_FANTASIA_ESTAB FROM TASY.ESTABELECIMENTO E WHERE E.CD_ESTABELECIMENTO = CP.CD_ESTABELECIMENTO)                           AS DS_ESTABELECIMENTO,
            -- PP.CD_MEDICO_EXECUTOR 																										    AS CD_MEDICO_EXECUTOR,
            NVL(PR.CD_MEDICO, PP.CD_MEDICO_EXECUTOR)                                                                                            AS CD_MEDICO_EXECUTOR,
            PR.CD_MEDICO																														AS CD_MEDICO_REPASSE,
            PP.CD_CGC_PRESTADOR 																												AS CD_CGC_PRESTADOR,
            PP.IE_FUNCAO_MEDICO 																												AS IE_FUNCAO_MEDICO,
            'A' 																																AS IE_PARTICIPOU_SUS,
            PP.IE_RESPONSAVEL_CREDITO 																											AS IE_RESPONSAVEL_CREDITO,
            AP.IE_TIPO_ATENDIMENTO 																												AS IE_TIPO_ATENDIMENTO,
            nvl(PP.IE_TIPO_ATO_SUS,0)				 																							AS IE_TIPO_ATO_SUS,
            nvl(PP.IE_TIPO_SERVICO_SUS,0)																										AS IE_TIPO_SERVICO_SUS,
            CP.NM_USUARIO_ORIGINAL 																												AS NM_USUARIO_ORIGINAL,
            nvl(TASY.OBTER_ETAPA_CHECKUP_PROC(PP.NR_SEQUENCIA),0) 																				AS NR_SEQ_ETAPA_CHECKUP,
            pr.VL_LIBERADO 																														AS VL_LIBERADO,
            PP.QT_PROCEDIMENTO 																													AS QT_PROCEDIMENTO,
            PP.VL_PROCEDIMENTO																													AS VL_PROCEDIMENTO,
            0 																																    AS VL_PARTICIPANTE,
            PP.VL_MEDICO 																														AS VL_MEDICO,
            PP.VL_ANESTESISTA 																													AS VL_ANESTESISTA,
            PP.VL_MATERIAIS 																													AS VL_MATERIAIS,
            PP.VL_AUXILIARES 																													AS VL_AUXILIARES,
            PP.VL_CUSTO_OPERACIONAL 																											AS VL_CUSTO_OPERACIONAL,
            tasy.obter_desc_prescr_proc_exam(pp.cd_procedimento, pp.ie_origem_proced, pp.NR_SEQ_PROC_INTERNO, pp.NR_SEQ_EXAME ) 				AS DS_PROC_INTERNO,
            tasy.obter_nome_pf_pj(NVL(PR.CD_MEDICO, PP.CD_MEDICO_EXECUTOR), null) 			 													AS NM_MEDICO_EXEC,
            TASY.OBTER_DESCRICAO_PADRAO('FUNCAO_MEDICO', 'DS_FUNCAO', PP.IE_FUNCAO_MEDICO) 														AS DS_FUNCAO_MEDICO,
            EP.CD_ESPECIALIDADE                                                                                                                 AS CD_ESPECIALIDADE,
            EP.DS_ESPECIALIDADE 																												AS DS_ESPECIALIDADE,
            cp.NR_PROTOCOLO 																													AS NR_PROTOCOLO,
            cp.NR_SEQ_PROTOCOLO 																												AS NR_SEQ_PROTOCOLO,
            TASY.OBTER_STATUS_PROTOCOLO(cp.NR_SEQ_PROTOCOLO) 																					AS IE_STATUS_PROTOCOLO,
            pc.dt_definitivo                                                                                                                    AS DT_DEFINITIVO,
            cp.IE_STATUS_ACERTO 																												AS IE_STATUS_ACERTO,
            tasy.obter_pessoa_atendimento(pp.nr_atendimento, 'N') 																				AS NM_PACIENTE,
            pp.CD_REGRA_REPASSE 																												AS CD_REGRA_REPASSE,
            pp.NR_SEQ_PROC_CRIT_REPASSE 																										AS NR_SEQ_CRITERIO_REPASSE,
            pr.nr_repasse_terceiro 																												AS NR_REPASSE_TERCEIRO,
            tasy.obter_nf_repasse(pr.nr_repasse_terceiro)  																						AS NR_NOTA_FISCAL,
            rnf.NR_SEQ_NOTA_FISCAL 																												AS NR_SEQ_NOTA_FISCAL,
            rt.DT_APROVACAO_TERCEIRO 																											AS DT_APROVACAO_TERCEIRO,
            TASY.OBTER_VALOR_CONTA(CP.NR_INTERNO_CONTA,0) 																						AS VL_CONTA,
            pr.vl_repasse 																														AS VL_REPASSE,
            TASY.OBTER_VALOR_ITEM_AUDIT(PP.NR_INTERNO_CONTA, PP.NR_SEQUENCIA, NULL, 'Q') 														AS ITEM_AUDIT,
            PP.CD_MOTIVO_EXC_CONTA 																												AS CD_MOTIVO_EXC_CONTA,
            p.cd_tipo_procedimento  																											AS CD_TIPO_PROCEDIMENTO,
            TASY.OBTER_VALOR_DOMINIO(95, p.CD_TIPO_PROCEDIMENTO)                                                                                AS DS_TIPO_PROCEDIMENTO,
            P.ds_procedimento 																													AS DS_PROCEDIMENTO,
            P.CD_GRUPO_PROC																														AS CD_GRUPO_PROC,
            tasy.obter_tipo_acomodacao(ap.nr_atendimento) 																						AS CD_TIPO_ACOMODACAO,
            tasy.Obter_se_atend_retorno(ap.nr_atendimento) 																						AS IE_ATENDIMENTO_RETORNO,
            mp.DS_MOTIVO_EXC_CONTA 																												AS DS_MOTIVO_EXC_CONTA,
            CP.DT_PERIODO_INICIAL 																												AS DT_PERIODO_INICIAL,
            CP.DT_PERIODO_FINAL    																												AS DT_PERIODO_FINAL,
            PR.NR_SEQ_ORIGEM																													AS NR_SEQ_ORIGEM,
            rt.IE_STATUS																														AS IE_STATUS,
            rt.DT_ULT_ENVIO_EMAIL																												AS DT_ULT_ENVIO_EMAIL,
            (SELECT e.nr_sequencia
            from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
            WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
            and f.nr_sequencia = cpe.nr_seq_etapa
            and f.nr_seq_classif = e.nr_sequencia
            and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     and x.dt_fim_etapa is null)) 												AS CD_ETAPA ,
            (SELECT e.ds_classificacao
             from TASY.conta_paciente_etapa cpe,
                  tasy.fatur_etapa f,
                  tasy.classif_etapa e
             WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
             and f.nr_sequencia = cpe.nr_seq_etapa
             and f.nr_seq_classif = e.nr_sequencia
             and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia = (select max(x.nr_sequencia)
                                                                  from tasy.conta_paciente_etapa x
                                                                  where cpe.nr_interno_conta = x.nr_interno_conta
                                                                  and x.dt_fim_etapa is null)) 												  	AS DS_ETAPA,
            (SELECT min(cpe.dt_etapa)
            from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
            WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
            and f.nr_sequencia = cpe.nr_seq_etapa
            and f.nr_seq_classif = e.nr_sequencia
            and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     and x.dt_fim_etapa is null)) 												AS DT_ETAPA,
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) 
             or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,pp.ie_funcao_medico) = pp.ie_funcao_medico
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'I') 																											AS REGRA_PACOTE_1,
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,pp.ie_funcao_medico) = pp.ie_funcao_medico
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'P')																											AS REGRA_PACOTE_2,
            (SELECT CE.nr_dias_venc_atend FROM TASY.convenio_estabelecimento CE 
             WHERE CP.CD_CONVENIO_PARAMETRO = CE.CD_CONVENIO AND cp.CD_ESTABELECIMENTO = CE.CD_ESTABELECIMENTO )                                AS NR_DIAS_VENC_ATEND,
            PP.CD_SITUACAO_GLOSA                                                                                                                AS CD_SITUACAO_GLOSA,
			(select 	nvl(max(IE_CONSISTE_SIT_GLOSA),'S') from TASY.parametro_faturamento where cd_estabelecimento =  AP.cd_estabelecimento)  AS IE_CONSISTE_SIT_GLOSA,
            ap.ie_clinica                                                                                                                       AS IE_CLINICA,
            (select max(v.dt_baixa_escritural) from tasy.dkz_pag_escritural_repasse_v v 
            where v.nr_repasse_terceiro = pr.nr_repasse_terceiro group by v.nr_repasse_terceiro)                                                AS DT_BAIXA_ESCRITURAL,
            CP.IE_CANCELAMENTO                                                                                                                  AS IE_CANCELAMENTO_CONTA,
            CASE WHEN PR.IE_STATUS = 'E' THEN 'E' ELSE 'N' END                                                                                  AS IE_STATUS_REPASSE,
            CP.DT_CONTA_PROTOCOLO                                                                                                               AS DT_CONTA_PROTOCOLO
        FROM
            TASY.conta_paciente CP,
            TASY.procedimento P,
            TASY.procedimento_repasse PR,
            TASY.procedimento_paciente PP,
            TASY.atendimento_paciente AP,
            TASY.ESPECIALIDADE_MEDICA EP,
            TASY.REPASSE_TERCEIRO rt,
            tasy.REPASSE_NOTA_FISCAL rnf,
            tasy.motivo_exc_conta_paciente mp,
            tasy.protocolo_convenio pc

        WHERE
            PP.nr_sequencia = PR.nr_seq_procedimento (+)
            AND PP.cd_procedimento = P.cd_procedimento AND PP.ie_origem_proced = p.ie_origem_proced
            AND PP.nr_atendimento = AP.nr_atendimento
            AND PP.nr_interno_conta = cp.nr_interno_conta
            AND pp.CD_ESPECIALIDADE = EP.CD_ESPECIALIDADE  (+)
            AND PR.NR_REPASSE_TERCEIRO = RT.NR_REPASSE_TERCEIRO (+)
            AND RT.NR_REPASSE_TERCEIRO = rnf.NR_REPASSE_TERCEIRO (+)
            AND pp.cd_motivo_exc_conta = mp.cd_motivo_exc_conta (+)
            AND cp.NR_SEQ_PROTOCOLO = pc.NR_SEQ_PROTOCOLO (+)
            AND PR.nr_seq_partic(+) IS NULL
            AND PP.CD_MEDICO_EXECUTOR IS NOT NULL
            AND pr.dt_liberacao(+) is not null -- Apenas procedimentos liberados, desconsidera "rascunhos"
           
        UNION ALL

        SELECT
            PPART.NR_SEQUENCIA 																													AS NR_SEQ_PROCED,
            PR.NR_SEQUENCIA 																													AS NR_SEQ_PROCED_REPASSE,
            CAST(PPART.NR_SEQ_PARTIC AS INT) 																									AS NR_SEQ_PARTIC,
            PP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PP,
            CP.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_CP,
            PR.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_PR,
            rnf.DT_ATUALIZACAO																													AS DT_ATUALIZACAO_RNF,
            PPART.DT_ATUALIZACAO																												AS DT_ATUALIZACAO_PPART,
            RT.DT_ATUALIZACAO                                                                                                                   AS DT_ATUALIZACAO_RT,
            TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA)																								AS CD_TAXA,
            TASY.OBTER_DESC_ESTRUT_CONTA(TASY.SOMENTE_NUMERO(PP.IE_EMITE_CONTA))																AS DS_TAXA,
            PP.CD_SETOR_ATENDIMENTO 																											AS CD_SETOR_ATENDIMENTO,
            (SELECT DS_SETOR_ATENDIMENTO FROM TASY.SETOR_ATENDIMENTO SA WHERE PP.CD_SETOR_ATENDIMENTO = SA.CD_SETOR_ATENDIMENTO)                AS DS_SETOR_ATENDIMENTO,
            PP.CD_PROCEDIMENTO 																													AS CD_PROCEDIMENTO,
            PP.IE_ORIGEM_PROCED 																												AS IE_ORIGEM_PROCED,
            P.IE_CLASSIFICACAO																													AS IE_CLASSIFICACAO,
            PP.NR_PRESCRICAO 																													AS NR_PRESCRICAO,
            PP.NR_CIRURGIA																														AS NR_CIRURGIA,
            PP.NR_SEQ_PROC_PACOTE 																												AS NR_SEQ_PROC_PACOTE,
            PP.NR_ATENDIMENTO 																													AS NR_ATENDIMENTO,
            AP.CD_MEDICO_RESP 																													AS CD_MEDICO_RESP,
            PP.DT_CONTA 																														AS DT_CONTA,
            PP.DT_PROCEDIMENTO 																													AS DT_PROCEDIMENTO,
            TASY.OBTER_MEDICO_LAUDO_SEQUENCIA(PP.NR_LAUDO,'C') 																					AS CD_MEDICO_LAUDO,
            PP.NR_INTERNO_CONTA 																												AS NR_INTERNO_CONTA,
            PP.NR_SEQ_PROC_INTERNO 																												AS NR_SEQ_PROC_INTERNO,
            AP.DT_ALTA 																															AS DT_ALTA,
            CP.DT_MESANO_REFERENCIA 																											AS DT_MESANO_REFERENCIA,
            AP.DT_ENTRADA 																														AS DT_ENTRADA,
            CP.CD_CONVENIO_PARAMETRO 																											AS CD_CONVENIO_PARAMETRO,
            (SELECT C.DS_CONVENIO FROM tasy.CONVENIO C WHERE C.CD_CONVENIO = CP.CD_CONVENIO_PARAMETRO)                                          AS DS_CONVENIO,
            nvl(round(TASY.OBTER_EDICAO(CP.CD_ESTABELECIMENTO, CP.CD_CONVENIO_PARAMETRO, CP.CD_CATEGORIA_PARAMETRO, AP.DT_ENTRADA, NULL),0),0)  AS CD_EDICAO_AMB ,
            CP.CD_ESTABELECIMENTO 																												AS CD_ESTABELECIMENTO,
            (SELECT NM_FANTASIA_ESTAB FROM TASY.ESTABELECIMENTO E WHERE E.CD_ESTABELECIMENTO = CP.CD_ESTABELECIMENTO)                           AS DS_ESTABELECIMENTO,
            PPART.CD_PESSOA_FISICA 																												AS CD_MEDICO_EXECUTOR,
            PR.CD_MEDICO																														AS CD_MEDICO_REPASSE,
            PPART.CD_CGC 																														AS CD_CGC_PRESTADOR,
            PPART.IE_FUNCAO 																													AS IE_FUNCAO_MEDICO,
            PPART.IE_PARTICIPOU_SUS 																											AS IE_PARTICIPOU_SUS,
            PPART.IE_RESPONSAVEL_CREDITO 																										AS IE_RESPONSAVEL_CREDITO,
            AP.IE_TIPO_ATENDIMENTO 																												AS IE_TIPO_ATENDIMENTO,
            nvl(PPART.IE_TIPO_ATO_SUS,0) 																										AS IE_TIPO_ATO_SUS,
            nvl(PPART.IE_TIPO_SERVICO_SUS,0)																									AS IE_TIPO_SERVICO_SUS,
            CP.NM_USUARIO_ORIGINAL 																												AS NM_USUARIO_ORIGINAL,
            nvl(TASY.OBTER_ETAPA_CHECKUP_PROC(PP.NR_SEQUENCIA),0) 																				AS NR_SEQ_ETAPA_CHECKUP,
            pr.VL_LIBERADO																														AS VL_LIBERADO,
            PP.QT_PROCEDIMENTO 																													AS QT_PROCEDIMENTO,
            PP.VL_PROCEDIMENTO																													AS VL_PROCEDIMENTO,
            nvl(PPART.VL_PARTICIPANTE,0)																										AS VL_PARTICIPANTE,
            0 																																    AS VL_MEDICO,
            PP.VL_ANESTESISTA 																													AS VL_ANESTESISTA,
            PP.VL_MATERIAIS 																													AS VL_MATERIAIS,
            PP.VL_AUXILIARES 																													AS VL_AUXILIARES,
            PP.VL_CUSTO_OPERACIONAL																												AS VL_CUSTO_OPERACIONAL,
            tasy.obter_desc_prescr_proc_exam(pp.cd_procedimento, pp.ie_origem_proced, pp.NR_SEQ_PROC_INTERNO, pp.NR_SEQ_EXAME ) 				AS DS_PROC_INTERNO,
            tasy.obter_nome_pessoa_fisica(PPART.CD_PESSOA_FISICA, null)														AS NM_MEDICO_EXEC, 
            TASY.OBTER_DESCRICAO_PADRAO('FUNCAO_MEDICO', 'DS_FUNCAO', PPART.IE_FUNCAO) 															AS DS_FUNCAO_MEDICO,
            EP.CD_ESPECIALIDADE                                                                                                                 AS CD_ESPECIALIDADE,
            EP.DS_ESPECIALIDADE 																												AS DS_ESPECIALIDADE,
            cp.NR_PROTOCOLO 																													AS NR_PROTOCOLO,
            cp.NR_SEQ_PROTOCOLO 																												AS NR_SEQ_PROTOCOLO,
            TASY.OBTER_STATUS_PROTOCOLO(cp.NR_SEQ_PROTOCOLO) 																					AS IE_STATUS_PROTOCOLO,
            pc.dt_definitivo                                                                                                                    AS DT_DEFINITIVO,
            cp.IE_STATUS_ACERTO 																												AS IE_STATUS_ACERTO,
            tasy.obter_pessoa_atendimento(pp.nr_atendimento, 'N') 																				AS NM_PACIENTE,
            ppart.CD_REGRA_REPASSE 																												AS CD_REGRA_REPASSE,
            ppart.NR_SEQ_PROC_CRIT_REPASSE 																										AS NR_SEQ_CRITERIO_REPASSE,
            pr.nr_repasse_terceiro 																												AS NR_REPASSE_TERCEIRO, 
            tasy.obter_nf_repasse(pr.nr_repasse_terceiro) 																						AS NR_NOTA_FISCAL,
            rnf.NR_SEQ_NOTA_FISCAL 																												AS NR_SEQ_NOTA_FISCAL,
            rt.DT_APROVACAO_TERCEIRO 																											AS DT_APROVACAO_TERCEIRO,
            TASY.OBTER_VALOR_CONTA(CP.NR_INTERNO_CONTA,0) 																						AS VL_CONTA,
            pr.vl_repasse 																														AS VL_REPASSE,	
            TASY.OBTER_VALOR_ITEM_AUDIT(PP.NR_INTERNO_CONTA, PP.NR_SEQUENCIA, NULL, 'Q') 														AS ITEM_AUDIT,
            PP.CD_MOTIVO_EXC_CONTA 																												AS CD_MOTIVO_EXC_CONTA,
            p.cd_tipo_procedimento 																												AS CD_TIPO_PROCEDIMENTO,
            TASY.OBTER_VALOR_DOMINIO(95, p.CD_TIPO_PROCEDIMENTO)                                                                                AS DS_TIPO_PROCEDIMENTO,
            P.ds_procedimento 																													AS DS_PROCEDIMENTO,
            P.CD_GRUPO_PROC																														AS CD_GRUPO_PROC,
            tasy.obter_tipo_acomodacao(ap.nr_atendimento) 																						AS CD_TIPO_ACOMODACAO,
            tasy.Obter_se_atend_retorno(ap.nr_atendimento) 																						AS IE_ATENDIMENTO_RETORNO,
            mp.DS_MOTIVO_EXC_CONTA 																												AS DS_MOTIVO_EXC_CONTA,
            CP.DT_PERIODO_INICIAL 																												AS DT_PERIODO_INICIAL,
            CP.DT_PERIODO_FINAL  																												AS DT_PERIODO_FINAL,
            PR.NR_SEQ_ORIGEM																													AS NR_SEQ_ORIGEM,
            rt.IE_STATUS 																														AS IE_STATUS,
            rt.DT_ULT_ENVIO_EMAIL																												AS DT_ULT_ENVIO_EMAIL,
            (SELECT e.nr_sequencia
                 from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
                 WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
                 AND f.nr_sequencia = cpe.nr_seq_etapa
                 AND f.nr_seq_classif = e.nr_sequencia
                 AND cpe.dt_fim_etapa is NULL AND cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     AND x.dt_fim_etapa is null)) 												AS CD_ETAPA,
            (SELECT e.ds_classificacao
                 from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
                 WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
                 AND f.nr_sequencia = cpe.nr_seq_etapa
                 AND f.nr_seq_classif = e.nr_sequencia
                 AND cpe.dt_fim_etapa is NULL AND cpe.nr_sequencia = (select max(x.nr_sequencia)
                                                                      from tasy.conta_paciente_etapa x
                                                                      where cpe.nr_interno_conta = x.nr_interno_conta
                                                                      AND x.dt_fim_etapa is null)) 												AS DS_ETAPA,
            (SELECT min(cpe.dt_etapa)
            from TASY.conta_paciente_etapa cpe,
                      tasy.fatur_etapa f,
                      tasy.classif_etapa e
            WHERE  cp.nr_interno_conta = cpe.nr_interno_conta
            and f.nr_sequencia = cpe.nr_seq_etapa
            and f.nr_seq_classif = e.nr_sequencia
            and cpe.dt_fim_etapa is NULL and cpe.nr_sequencia= (select max(x.nr_sequencia)
                                                                     from tasy.conta_paciente_etapa x
                                                                     where cpe.nr_interno_conta = x.nr_interno_conta
                                                                     and x.dt_fim_etapa is null)) 												AS DT_ETAPA,
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) 
             or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,ppart.ie_funcao) = ppart.ie_funcao
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'I')																											AS REGRA_PACOTE_1,		
            (SELECT max(1)
             from tasy.proc_criterio_repasse pcr
             where ((pcr.nr_seq_proc_interno = pp.nr_seq_proc_interno) 
             or (pcr.cd_procedimento = pp.cd_procedimento and pcr.ie_origem_proced = pp.ie_origem_proced))
             and nvl(pcr.ie_funcao,ppart.ie_funcao) = ppart.ie_funcao
             and nvl(pcr.cd_convenio,pp.cd_convenio) = pp.cd_convenio
             and pcr.ie_pacote = 'P')																											AS REGRA_PACOTE_2,
            (SELECT CE.nr_dias_venc_atend FROM TASY.convenio_estabelecimento CE 
             WHERE CP.CD_CONVENIO_PARAMETRO = CE.CD_CONVENIO AND cp.CD_ESTABELECIMENTO = CE.CD_ESTABELECIMENTO )                                AS NR_DIAS_VENC_ATEND,
             PP.CD_SITUACAO_GLOSA                                                                                                               AS CD_SITUACAO_GLOSA,
            (select nvl(max(IE_CONSISTE_SIT_GLOSA),'S') from TASY.parametro_faturamento where cd_estabelecimento =  AP.cd_estabelecimento)      AS IE_CONSISTE_SIT_GLOSA,
             ap.ie_clinica                                                                                                                      AS IE_CLINICA,
            (select max(v.dt_baixa_escritural) from tasy.dkz_pag_escritural_repasse_v v 
            where v.nr_repasse_terceiro = pr.nr_repasse_terceiro group by v.nr_repasse_terceiro)                                                AS DT_BAIXA_ESCRITURAL,
            CP.IE_CANCELAMENTO                                                                                                                  AS IE_CANCELAMENTO_CONTA,
            CASE WHEN PR.IE_STATUS = 'E' THEN 'E' ELSE 'N' END                                                                                  AS IE_STATUS_REPASSE,
            CP.DT_CONTA_PROTOCOLO                                                                                                               AS DT_CONTA_PROTOCOLO
        FROM
            TASY.conta_paciente CP,
            TASY.procedimento P,
            TASY.procedimento_participante PPART ,
            TASY.procedimento_repasse PR,
            TASY.procedimento_paciente PP,
            TASY.atendimento_paciente AP,
            TASY.ESPECIALIDADE_MEDICA EP,
            TASY.REPASSE_TERCEIRO rt,
            tasy.REPASSE_NOTA_FISCAL rnf,
            tasy.motivo_exc_conta_paciente mp,
            tasy.protocolo_convenio pc
        WHERE
            PP.nr_sequencia = PPART.NR_SEQUENCIA
            AND (PP.cd_procedimento = P.cd_procedimento AND PP.ie_origem_proced = P.ie_origem_proced)
            AND PP.nr_atendimento = AP.nr_atendimento
            AND PP.nr_interno_conta = CP.nr_interno_conta
            AND PR.nr_seq_procedimento(+) = PPART.nr_sequencia
            AND PR.nr_seq_partic(+) = PPART.nr_seq_partic
            AND PPART.CD_ESPECIALIDADE = EP.CD_ESPECIALIDADE  (+)
            AND PR.NR_REPASSE_TERCEIRO = RT.NR_REPASSE_TERCEIRO (+)
            AND RT.NR_REPASSE_TERCEIRO = rnf.NR_REPASSE_TERCEIRO (+)
            AND pp.cd_motivo_exc_conta = mp.cd_motivo_exc_conta (+)
            AND cp.NR_SEQ_PROTOCOLO = pc.NR_SEQ_PROTOCOLO (+)
            AND pr.dt_liberacao(+) is not null -- Apenas procedimentos liberados, desconsidera "rascunhos"
            AND PPART.CD_PESSOA_FISICA IS NOT NULL
        )

        SELECT 
        t.*,
        nvl(tab2.vl_estorno,0) as VL_ESTORNO,
        case when tab2.vl_estorno is not null then  'S' else 'N' end IE_ESTORNO
        FROM PROD_MED t
        left join (select 
                       nr_sequencia as NR_SEQ_PROCED_REPASSE,
                       (select max(vl_liberado) from tasy.procedimento_repasse x 
                       where dt_liberacao is not null
                           and x.ie_status = 'E'
                           and x.nr_repasse_terceiro = a.nr_repasse_terceiro
                           and x.nr_seq_procedimento = a.nr_seq_procedimento
                           and a.nr_seq_partic = x.nr_seq_partic
                           and nvl(x.cd_medico,x.nr_seq_partic) = nvl(a.cd_medico,a.nr_seq_partic)
                       ) as  VL_ESTORNO
                   from tasy.procedimento_repasse a
                   where 1=1
                   --a.ie_status <> 'E'
                   and dt_liberacao is not null
                   -- VERIFICA SE EXISTE O PROCEDIMENTO DUPLICADO DENTRO DE UM MESMO REPASSE 
                   /*and nr_repasse_terceiro = (select min(x.nr_repasse_terceiro)
                                                   from tasy.procedimento_repasse x
                                              where x.nr_seq_procedimento = a.nr_seq_procedimento
                                                    and x.cd_medico = a.cd_medico
                                                    and x.dt_liberacao is not null
                                                    and nvl(x.cd_medico,x.nr_seq_partic) = nvl(a.cd_medico,a.nr_seq_partic))*/
                                                    )tab2 on tab2.NR_SEQ_PROCED_REPASSE = t.NR_SEQ_PROCED_REPASSE
        WHERE
        CD_TAXA NOT IN (72, 73,74) -- gasoterapia, diárias, taxa
        AND NVL(CD_TIPO_PROCEDIMENTO,0) NOT IN (85) --NVL pois tem valores nulos --(20 = Laboratório, 37 = Diária, 38 = Taxa,  85 = Fisioterapia, 133 = Curativo) 
        AND CD_SETOR_ATENDIMENTO != 30 -- Laboratório
        AND CD_ESPECIALIDADE NOT IN( 102, 101, 108) --Fisioterapia, Fonodiaulogia, Bioquimico
        AND IE_CANCELAMENTO_CONTA IS NULL -- APENAS CONTAS NÃO CANCELADAS
        AND CD_PROCEDIMENTO <> 10104020 --Atendimento medico do intensivista em UTI geral ou pediatrica (plantao de 12 horas - por paciente)
        AND IE_STATUS_REPASSE <> 'E' --Exclui itens estornados
        AND CD_MEDICO_EXECUTOR IN {list_cd_pessoa_fisica_medicos}
        --AND CD_MEDICO_EXECUTOR IN (360, 642, 643, 191097, 231689, 257429, 433334, 231750, 229454, 231751, 636, 809, 595, 191097, 431198, 422, 810, 422187, 706, 
        --                           591,649,648,590,302381,254837,228923,231935,724,438517,532,231567,901,589,318376,592,447571,300260,394414,443521,275227,321493,441569,194772,233144,415015,199822,272957,549,652,239328,444757,424417,258586,659,231739,354274,228731,653,658,434209,395352,401046,368528,326954,572 )
        AND GREATEST(NVL(trunc(DT_ATUALIZACAO_PP,'MI'),  TO_DATE('1989-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) ,
				 NVL(trunc(DT_ATUALIZACAO_CP,'MI'),  TO_DATE('1989-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) ,
				 NVL(trunc(DT_ATUALIZACAO_PR,'MI'),  TO_DATE('1989-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) ,
				 NVL(trunc(DT_ATUALIZACAO_RNF,'MI'),  TO_DATE('1989-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) ,
				 NVL(trunc(DT_ATUALIZACAO_PPART,'MI'), TO_DATE('1989-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) ) BETWEEN TO_DATE('{dt_inicio_carga}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{dt_fim_carga}', 'YYYY-MM-DD HH24:MI:SS')
        AND  EXTRACT(YEAR FROM DT_PROCEDIMENTO) >= 2025
        -- AND  CD_ESPECIALIDADE IN (2,12,20,88,4) -- Lista de especialidades médicas
                                                --    2  -- Ortopedia e Traumatologia
                                                --    12 -- Oftalmologia
                                                --    20 --Ginecologia e Obstetrícia
                                                --    88 -- Ginecologia e obstetrícia (Inativo)
                                                --    4  -- Cirurgia Geral
    """