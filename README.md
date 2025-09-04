# AI
CREATE OR REPLACE VIEW GFIN_FAS_DS_IS_FR_Q_V AS
WITH cmt AS (
  SELECT c.bu, c.m_date, c.is_item_o, c.qoq, c.qoq_r, c.bpgap, c.bpgap_r, c.comments, c.comments_bp
  FROM gds.gfin_fas_ds_is_dmr_comment_v c
  WHERE c.fr_type = 'QOQ'
    AND c.current_period = (SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')
    AND exists (select 1 from gfin.gfin_fas_ds_is_bu_v bu
                 where bu.bg in ('BWR_Display','BWR_Mobility','DSBG','CNBU','ADP','MSBG','ND','DP1','DH1','MSBG_US','MSBG_EU','MS_EU','MS_US','BHTC_EU','BHTC_US','MS','ODM','LEDBU','DS_DV','MS_NU')
                   and c.bu = bu.tableau_bu)
),
 as_bu AS (
  SELECT b.*, r.type_rule
  FROM gfin.gfin_fas_ds_is_bu_v b,
       (SELECT * FROM gds.gfin_fas_ds_is_rule_c ru WHERE ru.data_type = 'ADJ' AND ru.report_rule = 'DMR' AND ru.is_active ='Y' ) r
  WHERE b.bu = r.bu(+)
    AND b.bg in ('BWR_Display','BWR_Mobility','DSBG','CNBU','ADP','MSBG','ND','DP1','DH1','MSBG_US','MSBG_EU','MS_EU','MS_US','BHTC_EU','BHTC_US','AUO','MS','ODM','LEDBU','DS_DV','MS_NU')
),
current_period AS (
  SELECT distinct max(to_date(a.month||'01','yyyymmdd')) current_period
  FROM gds.gfin_fas_ds_is_dtl_fix a
  WHERE a.act_fcst = 'Actual'  --AND a.bu ='MSBG_US'
),
fcst_max_month AS (
  SELECT max(to_date(a.month||'01','yyyymmdd')) fcst_max_month, bu.tableau_bu bu  --,a.bu  --20250604 modify
  FROM gds.gfin_fas_ds_is_dtl_fix a
  JOIN as_bu bu ON a.bu = bu.bu
  WHERE a.act_fcst = 'Forecast'
  GROUP BY bu.tableau_bu--, a.bu
),
version_date AS (
  SELECT distinct max(a.version_date) version_date
  FROM gds.gfin_fas_ds_is_dtl_fix a
  WHERE a.bu NOT IN ('MS_ADP','MSBG','BHTC','MSBG_INTERNAL')
)
SELECT Q.ACT_FCST, Q.YEAR, Q.QUARTER, current_period.current_period, Q.BU, Q.IS_ITEM,
       q.VALUE,
       Q.M_DATE,
       q.bp,
       q.bp_gap,
       fcst_max_month.fcst_max_month,
       version_date.version_date,
       M.QOQ, M.QOQ_R, M.BPGAP, M.BPGAP_R, M.COMMENTS, M.COMMENTS_BP
from (

        select a.act_fcst, a.year, a.quarter, --a.current_period,
               a.bu, a.is_item,
               decode(a.is_item, 'Sales_QTY', round(sum(a.value)/1000,0), round(sum(a.value)/1000000,0)) value,
               a.m_date,
               decode(a.is_item, 'Sales_QTY', round(sum(nvl(a.bp,0))/1000,0), round(sum(nvl(a.bp,0))/1000000,0)) bp ,
               decode(a.is_item, 'Sales_QTY', round(sum(a.value)/1000-sum(nvl(a.bp,0))/1000,0), round(sum(a.value)/1000000-sum(nvl(a.bp,0))/1000000,0))  bp_gap
               --a.fcst_max_month
        from (
                select 'Actual' act_fcst,
                       t.year,
                       t.quarter,
                       --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                       bu.tableau_bu bu,--t.bu,
                       t.is_item,
                       t.value,
                       trunc(t.m_date,'Q') m_date,
                       bp.value bp
                      --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
                from gfin_fas_ds_is_dtl_fix t
                     ,gds.auo_bp_fin_is_v bp
                     ,as_bu bu
                where t.is_item in ('Gross Profit','Operating Profit','Net Profit','Contribution Profit','OP+D'||chr(38)||'A','Revenue','Sales_QTY')
                and t.month = bp.month(+)
                and t.is_item = bp.is_item(+)
                and t.bu = bp.bu(+)
                and t.bu = bu.bu
                and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
                AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
                AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND t.bu = 'DV' AND t.year < 2025) --202501 LEDBU組織異動
                AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動

                union all

                --BP
                select 'Actual' act_fcst,
                       substr(i.month,1,4) year,
                       substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                       --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                       bu.tableau_bu bu,--t.bu,
                       i.is_item,
                       i.value,
                       trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                       i.value bp
                       --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
                from gds.auo_bp_fin_is_v i
                     ,as_bu bu
                     ,fcst_max_month x
                where  i.is_item in ('Gross Profit','Operating Profit','Net Profit','Contribution Profit','OP+D'||chr(38)||'A','Revenue','Sales_QTY')
                  and i.bu = bu.bu
                  AND i.bu = x.bu(+)
                  and to_date(i.month, 'yyyymm') > x.fcst_max_month
                  AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
                  AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND i.bu = 'DV' AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
                  AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
             )  a
         group by a.act_fcst, a.year, a.quarter, --a.current_period,
               a.bu, a.is_item,
               a.is_item,
               a.m_date
               --a.fcst_max_month

        union all
        --Area K_m2 data

        select a.act_fcst, a.year, a.quarter, --a.current_period,
               a.bu, a.is_item,
               decode(a.bu ,'ND',round(sum(a.value), 0),round(sum(a.value)/1000, 1)) value,
               a.m_date,
               decode(a.bu ,'ND',round(sum(nvl(a.bp,0)), 0),round(sum(nvl(a.bp,0))/1000, 1)) bp ,
               decode(a.bu ,'ND',round(sum(a.value) - sum(nvl(a.bp,0)), 0),round(sum(a.value)/1000 -sum(nvl(a.bp,0))/1000, 1))  bp_gap --,a.fcst_max_month
        from (
                select 'Actual'  act_fcst,
                       t.year,
                       t.quarter,
                       --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                       bu.tableau_bu bu,--t.bu,
                       'Area K_m2' is_item,
                       nvl(t.area,0) value,
                       trunc(t.m_date,'Q') m_date,
                       nvl(bp.value,0) bp
                       --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
                from gfin_fas_ds_is_dtl_fix t
                     ,gds.auo_bp_fin_is_v bp
                     ,as_bu bu
                where t.is_item in ('Revenue')
                and t.month = bp.month(+)
                and t.bu = bp.bu(+)
                and bp.is_item(+) = 'Sales_Area'
                and t.bu = bu.bu
                and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
                --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
                AND t.bu NOT IN ('MS_ADP', 'BHTC','BHTC_EU','BHTC_US') --202404 BHTC合併調整
                AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動

                union all
                --BP
                --Area K_m2 data
                select 'Actual' act_fcst,
                       substr(i.month,1,4) year,
                       substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                       ---(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                       bu.tableau_bu,--t.bu,
                       'Area K_m2' is_item,
                       nvl(i.value,0) ,
                       trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                       i.value bp
                       --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
                from gds.auo_bp_fin_is_v i
                     ,as_bu bu
                     ,fcst_max_month x
                where i.is_item = 'Sales_Area'
                  and i.bu = bu.bu
                  AND i.bu = x.bu(+)
                  and to_date(i.month, 'yyyymm') > x.fcst_max_month
                  --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
                  AND i.bu NOT IN ('MS_ADP', 'BHTC','BHTC_EU','BHTC_US') --202404 BHTC合併調整
                  AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
                ) a
         group by a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,    a.m_date--,   a.fcst_max_month



        union all
         --ASP m2 data

        select a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,
               round(decode(sum(a.area),0,0,sum(a.value)/sum(a.area)),0) value,
               a.m_date,
               round(decode(sum(nvl(a.area_bp,0)),0,0,sum(nvl(a.bp,0))/sum(nvl(a.area_bp,0))),0) bp ,
               round(decode(sum(a.area),0,0,sum(a.value)/sum(a.area))-decode(sum(nvl(a.area_bp,0)),0,0,sum(nvl(a.bp,0))/sum(nvl(a.area_bp,0))),0)  bp_gap
               --a.fcst_max_month
        from (
              select 'Actual'  act_fcst,
                     t.year,
                     t.quarter,
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu bu,--t.bu,
                     'ASP m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(t.value)
                          ELSE sum(decode(t.rate,0,0,t.value/t.rate)) END AS value,
                     trunc(t.m_date,'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(nvl(bp.value,0))
                          ELSE sum(decode(nvl(r.rate,0),0,0,nvl(bp.value,0)/nvl(r.rate,0))) END AS bp,
                     sum(nvl(t.area,0)) area,
                     sum(nvl(ab.area_bp,0)) area_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gfin_fas_ds_is_dtl_fix t
                   ,gds.auo_bp_fin_is_v bp
                   ,as_bu bu
                   ,(select b.month, b.bu, b.value area_bp
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'Sales_Area' ) ab
                   , gfin_fas_ds_bp_rate r
              where t.is_item in ('Revenue')
              and t.month = bp.month(+)
              and t.is_item = bp.is_item(+)
              and t.bu = bp.bu(+)
              and t.bu = bu.bu
              and ab.month(+) = t.month
              and ab.bu(+) = t.bu
              and t.year = r.year(+)
              and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
              --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
              AND t.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
              group by t.year,
                       t.quarter,
                       bu.tableau_bu,--t.bu,
                       trunc(t.m_date,'Q')

              union all
              --ASP m2 data
              select 'Actual' act_fcst,
                     substr(i.month,1,4) year,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu,--t.bu,
                     'ASP m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS value,
                     trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS bp,
                      sum(nvl(ab.area_bp,0))  area,
                      sum(nvl(ab.area_bp,0)) area_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gds.auo_bp_fin_is_v i
                   ,as_bu bu
                   ,fcst_max_month x
                   ,(select b.month, b.bu, b.value area_bp
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'Sales_Area' ) ab
                   , gfin_fas_ds_bp_rate r
              where i.is_item = 'Revenue'
                and i.bu = bu.bu
                AND i.bu = x.bu(+)
                and to_date(i.month, 'yyyymm') > x.fcst_max_month
                and ab.month = i.month
                and ab.bu = i.bu
                and substr(i.month,1,4) = r.year(+)
                --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
                AND i.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
                AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              group by  substr(i.month,1,4) ,
                       substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') ,
                       bu.tableau_bu,--t.bu,
                       trunc(to_date(i.month, 'yyyymm'),'Q')

              )a
         group by a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,    a.m_date--,   a.fcst_max_month



        union all
        --OP+D&A m2  data


        select a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,
               round(decode(sum(a.area),0,0,sum(a.rev_usd)/sum(a.area)) *decode(sum(a.rev),0,0,sum(a.OP_DA)/sum(a.rev)), 1) value,
               a.m_date,
               round(decode(sum(nvl(a.area_bp,0)),0,0,sum(a.rev_usd_bp)/sum(a.area_bp)) *decode(sum(nvl(a.rev_bp,0)),0,0,sum(a.OP_DA_BP)/sum(a.rev_bp)), 1) bp ,
               round(decode(sum(a.area),0,0,sum(a.rev_usd)/sum(a.area)) *decode(sum(a.rev),0,0,sum(a.OP_DA)/sum(a.rev)) - decode(sum(nvl(a.area_bp,0)),0,0,sum(a.rev_usd_bp)/sum(a.area_bp)) *decode(sum(nvl(a.rev_bp,0)),0,0,sum(a.OP_DA_BP)/sum(a.rev_bp)), 1) bp_gap
               --a.fcst_max_month
        from (
              select 'Actual'  act_fcst,
                     t.year,
                     t.quarter,
                    --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu bu,--t.bu,
                     'OP+D&A m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(t.value)
                          ELSE sum(decode(t.rate,0,0,t.value/t.rate)) END AS rev_usd,
                     sum(e.OP_DA) OP_DA,
                     sum(t.value) rev,
                     trunc(t.m_date,'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(nvl(bp.value,0))
                          ELSE sum(decode(nvl(r.rate,0),0,0,nvl(bp.value,0)/nvl(r.rate,0))) END AS rev_usd_bp,
                     sum(e.OP_DA_BP) OP_DA_BP,
                     sum(nvl(bp.value,0)) rev_bp,
                     sum(nvl(t.area,0)) area,
                     sum(nvl(ab.value,0)) area_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gfin_fas_ds_is_dtl_fix t
              LEFT JOIN gds.auo_bp_fin_is_v bp ON t.month = bp.month and t.bu = bp.bu and t.is_item = bp.is_item
              JOIN as_bu bu ON t.bu = bu.bu
              LEFT JOIN (select a.month, a.bu, bu.tableau_bu, a.value OP_DA, nvl(b.value,0) OP_DA_BP
                           from gfin_fas_ds_is_dtl_fix a
                           LEFT JOIN gds.auo_bp_fin_is_v b ON a.month = b.month and a.bu = b.bu and a.is_item = b.is_item
                           JOIN as_bu bu ON a.bu = bu.bu
                          where a.is_item = 'OP+D'||chr(38)||'A'
                            and ((bu.type_rule = 'ADJ' AND a.act_fcst = 'Forecast') OR bu.type_rule IS NULL )
                            AND a.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
                            AND NOT (bu.tableau_bu in ('BWR_Display') AND a.bu in (/*'ND',*/'GD','PD') AND a.year < 2025) --202501 LEDBU組織異動
                            --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND a.bu = 'BHTC' AND a.month < '202404') --202404 BHTC合併調整
                         ) e ON t.month = e.month and t.bu = e.bu and bu.tableau_bu = e.tableau_bu    --DMR調整後 20240215
              LEFT JOIN gds.auo_bp_fin_is_v ab ON t.month = ab.month and t.bu = ab.bu and ab.is_item = 'Sales_Area'
              LEFT JOIN gfin_fas_ds_bp_rate r ON t.year = r.year
              where t.is_item in ('Revenue')
              and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
              --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
              AND t.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
              group by
                       t.year,
                       t.quarter,
                       bu.tableau_bu,--t.bu,
                       trunc(t.m_date,'Q')


              union all
              --OP+D&A m2  data
              select 'Actual' act_fcst,
                     substr(i.month,1,4) year,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                    --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu,--t.bu,
                     'OP+D&A m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS rev_usd,
                     sum(e.OP_DA) OP_DA,
                     sum(i.value) rev,
                     trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS rev_usd_bp,
                     sum(e.OP_DA) OP_DA_BP,
                     sum(i.value) rev_bp,
                     sum( nvl(ab.area_bp,0)) area,
                     sum( nvl(ab.area_bp,0)) area_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gds.auo_bp_fin_is_v i
                   ,as_bu bu
                   ,(select b.month, b.bu, b.value OP_DA
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'OP+D'||chr(38)||'A') e
                   ,fcst_max_month x
                   ,(select b.month, b.bu, b.value area_bp
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'Sales_Area'  ) ab
                    , gfin_fas_ds_bp_rate r
              where i.is_item = 'Revenue'
                and i.bu = bu.bu
                and i.month = e.month(+)
                and i.bu = e.bu(+)
                AND i.bu = x.bu(+)
                and to_date(i.month, 'yyyymm') > x.fcst_max_month
                and ab.month(+) = i.month
                and ab.bu(+) = i.bu
                and substr(i.month,1,4) = r.year(+)
                --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
                AND i.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
                AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              group by
                     substr(i.month,1,4) ,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') ,
                     bu.tableau_bu,
                      trunc(to_date(i.month, 'yyyymm'),'Q')
                ) a
         group by a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,    a.m_date--,   a.fcst_max_month



        union all
        --CM/GM/OM/OP+D&A%=============================================

        select a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,
               round(decode(sum(a.rev),0,0,sum(a.value)/sum(a.rev)),3)  value,
               a.m_date,
               round(decode(sum(nvl(a.rev_bp,0)),0,0,sum(nvl(a.bp,0))/sum(a.rev_bp)),3) bp ,
               round(decode(sum(a.rev),0,0,sum(a.value)/sum(a.rev)) - decode(sum(nvl(a.rev_bp,0)),0,0,sum(a.bp)/sum(a.rev_bp)),3)  bp_gap
               --a.fcst_max_month
        from (

              select 'Actual' act_fcst,
                     t.year,
                     t.quarter,
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu bu,--t.bu,
                     decode(t.is_item,'Gross Profit', 'GM', 'Operating Profit', 'OM', 'Net Profit', 'NM', 'Contribution Profit', 'CM','OP+D'||chr(38)||'A', 'OP+D'||chr(38)||'A%') is_item,
                     sum(t.value)  value,
                     sum(nvl(r.rev,0)) rev,
                     trunc(t.m_date,'Q') m_date,
                     sum(nvl(bp.value,0)) bp,
                     sum(nvl(r.rev_bp,0)) rev_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gfin_fas_ds_is_dtl_fix t
              LEFT JOIN gds.auo_bp_fin_is_v bp ON t.month = bp.month and t.bu = bp.bu and t.is_item = bp.is_item
              JOIN as_bu bu ON t.bu = bu.bu
              LEFT JOIN ( select t.month, t.bu, bu.tableau_bu , sum(t.value) rev, sum(i.value) rev_bp
                            from gfin_fas_ds_is_dtl_fix t
                            LEFT JOIN gds.auo_bp_fin_is_v i ON t.is_item = i.is_item and t.bu = i.bu and t.month = i.month
                            JOIN as_bu bu ON t.bu = bu.bu
                           where t.is_item = 'Revenue'
                             and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 202400215
                             AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
                             AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND t.bu = 'DV' AND t.year < 2025) --202501 LEDBU組織異動
                             AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
                           group by t.month, t.bu, bu.tableau_bu ) r ON t.month = r.month and t.bu = r.bu AND bu.tableau_bu = r.tableau_bu
              where t.is_item in ('Gross Profit','Operating Profit','Net Profit','Contribution Profit','OP+D'||chr(38)||'A')
              and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
              AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND t.bu = 'DV' AND t.year < 2025) --202501 LEDBU組織異動
              AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
              group by
                   t.year,
                   t.quarter,
                   bu.tableau_bu,--t.bu,
                   t.is_item,
                   trunc(t.m_date,'Q')

              union all
              --CM/GM/OM/OP+D&A%=============================================
              select 'Actual' act_fcst,
                     substr(i.month,1,4) year,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu bu,--t.bu,
                     decode(i.is_item, 'Gross Profit', 'GM', 'Operating Profit', 'OM', 'Net Profit', 'NM', 'Contribution Profit', 'CM','OP+D'||chr(38)||'A', 'OP+D'||chr(38)||'A%') is_item,
                     sum(i.value) value,
                     sum(r.rev) rev,
                     trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                     sum(i.value) bp,
                     sum(r.rev) rev_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gds.auo_bp_fin_is_v i
                 ,as_bu bu
                 ,(select i.month, i.bu, sum(i.value) rev
                     from gds.auo_bp_fin_is_v i
                    where i.is_item = 'Revenue'
                    group by i.month, i.bu )  r
                 ,fcst_max_month x
              where i.is_item in ('Gross Profit','Operating Profit','Net Profit','Contribution Profit','OP+D'||chr(38)||'A')
              and i.bu = bu.bu
              AND i.bu = x.bu(+)
              and to_date(i.month, 'yyyymm') > x.fcst_max_month
              and i.bu = r.bu(+)
              and i.month = r.month(+)
              AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND i.bu = 'DV' AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              group by
                   substr(i.month,1,4) ,
                   substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') ,
                   bu.tableau_bu ,--t.bu,
                   decode(i.is_item, 'Gross Profit', 'GM', 'Operating Profit', 'OM', 'Net Profit', 'NM', 'Contribution Profit', 'CM','OP+D'||chr(38)||'A', 'OP+D'||chr(38)||'A%') ,
                   trunc(to_date(i.month, 'yyyymm'),'Q')

                 ) a
         group by a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,    a.m_date--,   a.fcst_max_month



        union all
        --GAP ===================================================================

        select 'Gap' act_fcst,
               t.year,
               t.quarter,
               --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
               bu.tableau_bu bu,--t.bu,
               t.is_item,
               round(decode(t.is_item, 'Sales_QTY', sum(t.value)/1000, sum(t.value)/1000000) - decode(t.is_item,'Sales_QTY', sum(nvl(bp.value,0))/1000, sum(nvl(bp.value,0))/1000000),0)  value,
               trunc(t.m_date,'Q') m_date,
               decode(t.is_item, 'Sales_QTY', round(sum(bp.value)/1000,0), round(sum(bp.value)/1000000,0)) bp,
               round(decode(t.is_item, 'Sales_QTY', sum(t.value)/1000, sum(t.value)/1000000) - decode(t.is_item,'Sales_QTY', sum(nvl(bp.value,0))/1000, sum(nvl(bp.value,0))/1000000),0)  bp_gap
               --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
        from gfin_fas_ds_is_dtl_fix t
             ,gds.auo_bp_fin_is_v bp
             ,as_bu bu
        where t.is_item in ('Gross Profit', 'Operating Profit', 'Net Profit','Contribution Profit','OP+D'||chr(38)||'A','Revenue','Sales_QTY')
        and t.month = bp.month(+)
        and t.is_item = bp.is_item(+)
        and t.bu = bp.bu(+)
        and t.bu = bu.bu
        and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
        AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
        AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND t.bu = 'DV' AND t.year < 2025) --202501 LEDBU組織異動
        AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
        group by
               t.year,
               t.quarter,
               bu.tableau_bu,--t.bu,
               t.is_item,
               nvl(t.index_group, '-') ,
               trunc(t.m_date,'Q')/*,
               bp.is_item*/


        union all
        --Area K_m2 data

        select 'Gap'  act_fcst,
               t.year,
               t.quarter,
               --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
               bu.tableau_bu,--t.bu,
               'Area K_m2' is_item,
               decode(bu.tableau_bu ,'ND',round(sum(nvl(t.area,0)) - sum(nvl(bp.value,0)) , 0),round(sum(nvl(t.area,0))/1000 - sum(nvl(bp.value,0))/1000 , 1)) value,
               trunc(t.m_date,'Q') m_date,
               round(sum(nvl(bp.value,0))/1000, 1) bp,
               round(sum(nvl(t.area,0))/1000 - sum(nvl(bp.value,0))/1000 , 1) bp_gap
               --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
        from gfin_fas_ds_is_dtl_fix t
             ,gds.auo_bp_fin_is_v bp
             ,as_bu bu
        where t.is_item in ('Revenue')
        and t.month = bp.month(+)
        and t.bu = bp.bu(+)
        and bp.is_item(+)= 'Sales_Area'
        and t.bu = bu.bu
        and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
        --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
        AND t.bu NOT IN ('MS_ADP', 'BHTC','BHTC_EU','BHTC_US') --202404 BHTC合併調整
        AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
        group by
               t.year,
               t.quarter,
               bu.tableau_bu,--t.bu,
               trunc(t.m_date,'Q')

        union all
        --ASP m2 data

        select a.act_fcst, a.year, a.quarter,/* a.current_period,*/ a.bu, a.is_item,
               round(decode(sum(a.area),0,0,sum(a.value)/sum(a.area))-decode(sum(nvl(a.area_bp,0)),0,0,sum(nvl(a.bp,0))/sum(nvl(a.area_bp,0))),0) value,
               a.m_date,
               round(decode(sum(nvl(a.area_bp,0)),0,0,sum(nvl(a.bp,0))/sum(nvl(a.area_bp,0))),0) bp ,
               round(decode(sum(a.area),0,0,sum(a.value)/sum(a.area))-decode(sum(nvl(a.area_bp,0)),0,0,sum(nvl(a.bp,0))/sum(nvl(a.area_bp,0))),0)  bp_gap
               --a.fcst_max_month
        from (

              select 'Gap'  act_fcst,
                     t.year,
                     t.quarter,
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu bu,--t.bu,
                     'ASP m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(t.value)
                          ELSE sum(decode(t.rate,0,0,t.value/t.rate)) END AS value,
                     trunc(t.m_date,'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(nvl(bp.value,0))
                          ELSE sum(decode(nvl(r.rate,0),0,0,nvl(bp.value,0)/nvl(r.rate,0))) END AS bp,
                     sum(nvl(t.area,0)) area,
                     sum(nvl(ab.area_bp,0)) area_bp
                    --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gfin_fas_ds_is_dtl_fix t
                   ,gds.auo_bp_fin_is_v bp
                   ,as_bu bu
                   ,(select b.month, b.bu, b.value area_bp
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'Sales_Area' ) ab
                   , gfin_fas_ds_bp_rate r
              where t.is_item in ('Revenue')
              and t.month = bp.month(+)
              and t.is_item = bp.is_item(+)
              and t.bu = bp.bu(+)
              and t.bu = bu.bu
              and ab.month(+) = t.month
              and ab.bu(+) = t.bu
              and t.year = r.year(+)
              and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
              --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
              AND t.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
              group by t.year,
                       t.quarter,
                       bu.tableau_bu,--t.bu,
                       trunc(t.m_date,'Q')

              union all
              --ASP m2 data
              select 'Gap' act_fcst,
                     substr(i.month,1,4) year,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu,--t.bu,
                     'ASP m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS value,
                     trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS bp,
                     sum(nvl(ab.area_bp,0))  area,
                     sum(nvl(ab.area_bp,0)) area_bp
                    -- (SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gds.auo_bp_fin_is_v i
                   ,as_bu bu
                   ,fcst_max_month x
                   ,(select b.month, b.bu, b.value area_bp
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'Sales_Area' ) ab
                   , gfin_fas_ds_bp_rate r
              where i.is_item = 'Revenue'
                and i.bu = bu.bu
                AND i.bu = x.bu(+)
                and to_date(i.month, 'yyyymm') > x.fcst_max_month
                and ab.month(+) = i.month
                and ab.bu(+) = i.bu
                and substr(i.month,1,4) = r.year(+)
                --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
                AND i.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
                AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              group by  substr(i.month,1,4) ,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') ,
                     bu.tableau_bu,--t.bu,
                     trunc(to_date(i.month, 'yyyymm'),'Q')

              )a
         group by a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,    a.m_date--,   a.fcst_max_month



        union all
        --OP+D&A m2  data


        select a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,
               round(decode(sum(a.area),0,0,sum(a.rev_usd)/sum(a.area)) *decode(sum(a.rev),0,0,sum(a.OP_DA)/sum(a.rev)) - decode(sum(nvl(a.area_bp,0)),0,0,sum(a.rev_usd_bp)/sum(a.area_bp)) *decode(sum(a.rev_bp),0,0,sum(a.OP_DA_BP)/sum(a.rev_bp)), 1) value,
               a.m_date,
               round(decode(sum(a.area_bp),0,0,sum(a.rev_usd_bp)/sum(a.area_bp)) *decode(sum(nvl(a.rev_bp,0)),0,0,sum(a.OP_DA_BP)/sum(a.rev_bp)), 1) bp ,
               round(decode(sum(a.area),0,0,sum(a.rev_usd)/sum(a.area)) *decode(sum(a.rev),0,0,sum(a.OP_DA)/sum(a.rev)) - decode(sum(nvl(a.area_bp,0)),0,0,sum(a.rev_usd_bp)/sum(a.area_bp)) *decode(sum(a.rev_bp),0,0,sum(a.OP_DA_BP)/sum(a.rev_bp)), 1) bp_gap
               --a.fcst_max_month
        from (
              select 'Gap'  act_fcst,
                     t.year,
                     t.quarter,
                    --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu bu,--t.bu,
                     'OP+D&A m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(t.value)
                          ELSE sum(decode(t.rate,0,0,t.value/t.rate)) END AS rev_usd,
                     sum(e.OP_DA) OP_DA,
                     sum(t.value) rev,
                     trunc(t.m_date,'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(nvl(bp.value,0))
                          ELSE sum(decode(nvl(r.rate,0),0,0,nvl(bp.value,0)/nvl(r.rate,0))) END AS rev_usd_bp,
                     sum(nvl(e.OP_DA_BP,0)) OP_DA_BP,
                     sum(nvl(bp.value,0)) rev_bp,
                     sum(nvl(t.area,0)) area,
                     sum(nvl(ab.value,0)) area_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gfin_fas_ds_is_dtl_fix t
              LEFT JOIN gds.auo_bp_fin_is_v bp ON t.month = bp.month and t.bu = bp.bu and t.is_item = bp.is_item
              JOIN as_bu bu ON t.bu = bu.bu
              LEFT JOIN (select a.month, a.bu, bu.tableau_bu, a.value OP_DA, nvl(b.value,0) OP_DA_BP
                           from gfin_fas_ds_is_dtl_fix a
                           LEFT JOIN gds.auo_bp_fin_is_v b ON a.month = b.month and a.bu = b.bu and a.is_item = b.is_item
                           JOIN as_bu bu ON a.bu = bu.bu
                          where a.is_item = 'OP+D'||chr(38)||'A'
                            and ((bu.type_rule = 'ADJ' AND a.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
                            --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND a.bu = 'BHTC' AND a.month < '202404') --202404 BHTC合併調整
                            AND a.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
                            AND NOT (bu.tableau_bu in ('BWR_Display') AND a.bu in (/*'ND',*/'GD','PD') AND a.year < 2025) --202501 LEDBU組織異動
                         ) e ON t.month = e.month and t.bu = e.bu and bu.tableau_bu = e.tableau_bu    --DMR調整後 20240215
              LEFT JOIN gds.auo_bp_fin_is_v ab ON t.month = ab.month and t.bu = ab.bu and ab.is_item = 'Sales_Area'
              LEFT JOIN gfin_fas_ds_bp_rate r ON t.year = r.year
              where t.is_item in ('Revenue')
              and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
              --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
              AND t.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
              group by
                       t.year,
                       t.quarter,
                       bu.tableau_bu,--t.bu,
                       trunc(t.m_date,'Q')


              union all
              --OP+D&A m2  data
              select 'Gap' act_fcst,
                     substr(i.month,1,4) year,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                    --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu,--t.bu,
                     'OP+D&A m2' is_item,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS rev_usd,
                     sum(e.OP_DA) OP_DA,
                     sum(i.value) rev,
                     trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                     CASE WHEN bu.tableau_bu IN ('MSBG_US','MS_US','MSBG_EU','MS_EU') THEN sum(i.value)
                          ELSE sum(decode(r.rate,0,0,i.value/r.rate)) END AS rev_usd_bp,
                     sum(e.OP_DA) OP_DA_BP,
                     sum(i.value) rev_bp,
                     sum( nvl(ab.area_bp,0)) area,
                     sum( nvl(ab.area_bp,0)) area_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gds.auo_bp_fin_is_v i
                   ,as_bu bu
                   ,(select b.month, b.bu, b.value OP_DA
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'OP+D'||chr(38)||'A') e
                   ,fcst_max_month x
                   ,(select b.month, b.bu, b.value area_bp
                       from gds.auo_bp_fin_is_v b
                      where b.is_item = 'Sales_Area'  ) ab
                   , gfin_fas_ds_bp_rate r
              where i.is_item = 'Revenue'
                and i.bu = bu.bu
                and i.month = e.month(+)
                and i.bu = e.bu(+)
                AND i.bu = x.bu(+)
                and to_date(i.month, 'yyyymm') > x.fcst_max_month
                and ab.month(+) = i.month
                and ab.bu(+) = i.bu
                and substr(i.month,1,4) = r.year(+)
                --AND NOT (bu.tableau_bu in ('AUO', 'MSBG') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
                AND i.bu NOT IN ('MS_ADP', 'BHTC','MSBG_US','MSBG_EU',/*'MS_EU','MS_US',*/'BHTC_EU','BHTC_US') --202404 BHTC合併調整
                AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              group by
                     substr(i.month,1,4) ,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') ,
                     bu.tableau_bu,--t.bu,
                     trunc(to_date(i.month, 'yyyymm'),'Q')
                ) a
         group by a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,    a.m_date--,   a.fcst_max_month




        union all
        --CM/GM/OM/OP+D&A%=============================================

        select a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,
               round(decode(sum(a.rev),0,0,sum(a.value)/sum(a.rev)) - decode(sum(nvl(a.rev_bp,0)),0,0,sum(nvl(a.bp,0))/sum(nvl(a.rev_bp,0))),3)  value,
               a.m_date,
               round(decode(sum(nvl(a.rev_bp,0)),0,0,sum(a.bp)/sum(a.rev_bp)),3) bp ,
               round(decode(sum(a.rev),0,0,sum(a.value)/sum(a.rev)) - decode(sum(nvl(a.rev_bp,0)),0,0,sum(nvl(a.bp,0))/sum(nvl(a.rev_bp,0))),3)  bp_gap
               --a.fcst_max_month
        from (

              select 'Gap' act_fcst,
                   t.year,
                   t.quarter,
                   --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                   bu.tableau_bu bu,  --t.bu,
                   decode(t.is_item, 'Gross Profit', 'GM', 'Operating Profit', 'OM', 'Net Profit', 'NM', 'Contribution Profit', 'CM','OP+D'||chr(38)||'A', 'OP+D'||chr(38)||'A%') is_item,
                   sum(t.value)  value,
                   sum(r.rev) rev,
                   trunc(t.m_date,'Q') m_date,
                   sum(bp.value) bp,
                   sum(r.rev_bp) rev_bp
                   --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gfin_fas_ds_is_dtl_fix t
              LEFT JOIN gds.auo_bp_fin_is_v bp ON t.month = bp.month and t.bu = bp.bu and t.is_item = bp.is_item
              JOIN as_bu bu ON t.bu = bu.bu
              LEFT JOIN ( select t.month, t.bu, bu.tableau_bu , sum(t.value) rev, sum(i.value) rev_bp
                            from gfin_fas_ds_is_dtl_fix t
                            LEFT JOIN gds.auo_bp_fin_is_v i ON t.is_item = i.is_item and t.bu = i.bu and t.month = i.month
                            JOIN as_bu bu ON t.bu = bu.bu
                           where t.is_item = 'Revenue'
                             and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 202400215
                             AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
                             AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND t.bu = 'DV' AND t.year < 2025) --202501 LEDBU組織異動
                           group by t.month, t.bu, bu.tableau_bu ) r ON t.month = r.month and t.bu = r.bu AND bu.tableau_bu = r.tableau_bu
              where t.is_item in ('Gross Profit','Operating Profit','Net Profit','Contribution Profit','OP+D'||chr(38)||'A')
              and ((bu.type_rule = 'ADJ' AND t.act_fcst = 'Forecast') OR bu.type_rule IS NULL )  --DMR調整後 20240215
              AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND t.bu = 'BHTC' AND t.month < '202404') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND t.bu = 'DV' AND t.year < 2025) --202501 LEDBU組織異動
              AND NOT (bu.tableau_bu in ('BWR_Display') AND t.bu in (/*'ND',*/'GD','PD') AND t.year < 2025) --202501 LEDBU組織異動
              group by
                   t.year,
                   t.quarter,
                   bu.tableau_bu,  --t.bu,
                   t.is_item,
                   trunc(t.m_date,'Q')

              union all
              --CM/GM/OM/OP+D&A%=============================================
              select 'Gap' act_fcst,
                     substr(i.month,1,4) year,
                     substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') quarter,
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Actual')  current_period,
                     bu.tableau_bu bu,--t.bu,
                     decode(i.is_item, 'Gross Profit', 'GM', 'Operating Profit', 'OM', 'Net Profit', 'NM', 'Contribution Profit', 'CM','OP+D'||chr(38)||'A', 'OP+D'||chr(38)||'A%') is_item,
                     sum(i.value) value,
                     sum(r.rev) rev,
                     trunc(to_date(i.month, 'yyyymm'),'Q') m_date,
                     sum(i.value) bp,
                     sum(r.rev) rev_bp
                     --(SELECT distinct max(to_date(a.month||'01','yyyymmdd')) m FROM gds.gfin_fas_ds_is_dtl_fix a where a.act_fcst = 'Forecast')  fcst_max_month
              from gds.auo_bp_fin_is_v i
                   ,as_bu bu
                   ,(select i.month, i.bu, sum(i.value) rev
                       from gds.auo_bp_fin_is_v i
                      where i.is_item = 'Revenue'
                      group by i.month, i.bu )  r
                   ,fcst_max_month x
              where i.is_item in ('Gross Profit','Operating Profit','Net Profit','Contribution Profit','OP+D'||chr(38)||'A')
              and i.bu = bu.bu
              AND i.bu = x.bu(+)
              and to_date(i.month, 'yyyymm') > x.fcst_max_month
              and i.bu = r.bu(+)
              and i.month = r.month(+)
              AND NOT (bu.tableau_bu in ('AUO', 'MSBG','BWR_3_Pillars','BWR_Mobility') AND i.bu = 'BHTC' AND i.month < '202404') --202404 BHTC合併調整
              AND NOT (bu.tableau_bu in ('LEDBU', 'LED', 'LED_SUM','DS_DV','MS_NU') AND i.bu = 'DV' AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              AND NOT (bu.tableau_bu in ('BWR_Display') AND i.bu in (/*'ND',*/'GD','PD') AND substr(i.month,1,4) < 2025) --202501 LEDBU組織異動
              group by
                   substr(i.month,1,4) ,
                   substr(i.month,1,4)||'Q'||to_char(to_date(i.month,'yyyymm'),'Q') ,
                   bu.tableau_bu ,--t.bu,
                   i.is_item,
                   trunc(to_date(i.month, 'yyyymm'),'Q')
                 ) a
         group by a.act_fcst, a.year, a.quarter, /*a.current_period,*/ a.bu, a.is_item,    a.m_date--,   a.fcst_max_month





)  q
LEFT JOIN cmt m ON m.bu = q.bu AND m.m_date = q.m_date AND m.is_item_o = q.is_item
CROSS JOIN current_period
CROSS JOIN version_date
LEFT JOIN fcst_max_month  ON fcst_max_month.bu = q.bu
where q.year >= TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE,'YYYY'),-24),'YYYY')
;
