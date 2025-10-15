# routes.py
from flask import Blueprint, request, jsonify
from datetime import datetime, timezone
from pydantic import ValidationError
from sqlalchemy import text
from Schema import Recommendation
from helper import ENGINE, get_published_address, normalize_record, run_query, insert_recommendations, call_gpt, insert_summary, send_summary_email, summary_prompt


import os, json
from dotenv import load_dotenv
load_dotenv()




TO_EMAILS = [
    "g.agyeabour@awcghana.com",
    "m.williams@awcghana.com",
    "patrick@awcghana.com",
    "s.namoafo@awcghana.com",
    "b.oagyemang@awcghana.com",
]

bp = Blueprint("api", __name__)

@bp.get("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.now(timezone.utc).isoformat()})

@bp.get("/data/sql")
def data_sql():
  sql = """
      ;WITH maxd AS (
    SELECT MAX(TRY_CONVERT(date, [As of Date])) AS max_date
    FROM [NPL].[dbo].[t_insightView_KRI]
)
SELECT
    t.[KRI ID]              AS relatedEntityId,
    t.[KRI_Name]            AS metricName,
    t.[Adjusted Current Mth] AS metricValue,
    t.[As of Date]          AS observedAt,
    t.[KRI Standard]        AS kriStandard,
    t.[Risk Type]           AS riskType,
    t.[RiskW]               AS riskW,
    t.[ImpactBin_Col]       AS impactLevel,
    t.[LikelihoodBin_Col]   AS likelihoodBin,
    t.[RiskLevel_Col]       AS probabilityLevel,
    t.[Warning Limit1]      AS warningLimit,
    t.[Warning Limit1 Operator] AS warningLimitOperator,
    t.[Escalaltion Limit 1 Num] AS escalationLimit,
    t.[Escalation Limit1 Operator] AS escalationLimitOperator,
    t.[Threshold_Value]     AS thresholdLimit,
    t.[Threshold_Operator]  AS thresholdOperator,
    t.[ExposureScoreCol]    AS exposureScore,
    t.[KRI Status]          AS statusBand,
    CASE
        WHEN t.[KRI Status] = 'Breached' THEN 2
        WHEN t.[KRI Status] = 'Warning'  THEN 1
        ELSE 0
    END AS breachLevel
FROM [NPL].[dbo].[t_insightView_KRI] t
WHERE
    t.[TOP_KRIs] = 1
    AND t.[Breached KRIs] > 0
    AND TRY_CONVERT(date, t.[As of Date]) 
        BETWEEN DATEADD(MONTH, -2, (SELECT max_date FROM maxd)) 
            AND (SELECT max_date FROM maxd)
ORDER BY
    breachLevel DESC,
    TRY_CONVERT(date, t.[As of Date]) DESC;

      """
  
  rows = run_query(sql)
  for r in rows:
    print(f"{r['relatedEntityId']} — {r['metricName']}: {r['metricValue']} | "
          f"status={r['statusBand']} | breachLevel={r['breachLevel']} | "
          f"asOf={r['observedAt']}")
  return jsonify({
        "meta": {"rows": len(rows), "window": "last_12_months_from_max_AsOfDate"},
        "data": {"source": "KRI", "rows": rows}
    })

@bp.post("/recommendations")
def post_recommendations():
    payload = request.get_json(force=True)
    items = payload if isinstance(payload, list) else [payload]
    validated = []
    for it in items:
        try:
            r = Recommendation(**it)
            rec = {
                "source": r.source,
                "relatedEntityId": r.relatedEntityId,
                "metricName": r.metricName,
                "metricValue": r.metricValue,
                "recommendationText": r.recommendationText,
                "actionType": r.actionType,
                "confidence": r.confidence,
                "riskType": r.riskType,
                "referenceTimestamp": r.referenceTimestamp,
                "observedAt": getattr(r, "observedAt", None) or datetime.today().date().isoformat(),
                "metadata": r.metadata or {},
                "postMitigationValue": getattr(r, "postMitigationValue", None)
            }
            validated.append(normalize_record(rec))

        except ValidationError as ve:
            return jsonify({"error": "validation_failed", "detail": ve.errors()}), 400
    inserted = insert_recommendations(validated)
    return jsonify({"inserted": inserted}), 201

@bp.post("/gpt/run")
def gpt_run():
    sql = """
        ;WITH maxd AS (
    SELECT MAX(TRY_CONVERT(date, [As of Date])) AS max_date
    FROM [NPL].[dbo].[t_insightView_KRI]
)
SELECT
    t.[KRI ID]              AS relatedEntityId,
    t.[KRI_Name]            AS metricName,
    t.[Adjusted Current Mth] AS metricValue,
    t.[As of Date]          AS observedAt,
    t.[KRI Standard]        AS kriStandard,
    t.[Risk Type]           AS riskType,
    t.[RiskW]               AS riskW,
    t.[ImpactBin_Col]       AS impactLevel,
    t.[LikelihoodBin_Col]   AS likelihoodBin,
    t.[RiskLevel_Col]       AS probabilityLevel,
    t.[Warning Limit1]      AS warningLimit,
    t.[Warning Limit1 Operator] AS warningLimitOperator,
    t.[Escalaltion Limit 1 Num] AS escalationLimit,
    t.[Escalation Limit1 Operator] AS escalationLimitOperator,
    t.[Threshold_Value]     AS thresholdLimit,
    t.[Threshold_Operator]  AS thresholdOperator,
    t.[ExposureScoreCol]    AS exposureScore,
    t.[KRI Status]          AS statusBand,
    CASE
        WHEN t.[KRI Status] = 'Breached' THEN 2
        WHEN t.[KRI Status] = 'Warning'  THEN 1
        ELSE 0
    END AS breachLevel
FROM [NPL].[dbo].[t_insightView_KRI] t
WHERE
    t.[TOP_KRIs] = 1
    AND t.[Breached KRIs] > 0
    AND TRY_CONVERT(date, t.[As of Date]) 
        BETWEEN DATEADD(MONTH, -2, (SELECT max_date FROM maxd)) 
            AND (SELECT max_date FROM maxd)
ORDER BY
    breachLevel DESC,
    TRY_CONVERT(date, t.[As of Date]) DESC;

        """

    rows = run_query(sql)
    compact = {"source": "KRI", "window": "year_2025", "rows": rows}
    recs = call_gpt(compact)

    validated, errors = [], []
    for it in recs:
        try:
            r = Recommendation(**it)
            rec = {
                "source": r.source,
                "relatedEntityId": r.relatedEntityId,
                "metricName": r.metricName,
                "metricValue": r.metricValue,
                "recommendationText": r.recommendationText,
                "actionType": r.actionType,
                "confidence": r.confidence,
                "riskType": r.riskType,
                "referenceTimestamp": r.referenceTimestamp,
                "observedAt": getattr(r, "observedAt", None) \
              or next((row["observedAt"] for row in rows if row["relatedEntityId"] == r.relatedEntityId), None),

                "metadata": r.metadata or {},
                "postMitigationValue": getattr(r, "postMitigationValue", None)
            }
            validated.append(normalize_record(rec))
        except Exception as e:
            errors.append({"item": it, "error": str(e)})
    count = insert_recommendations(validated) if validated else 0
    return jsonify({"generated": len(recs), "inserted":count,  "errors": errors,  "recommendations": validated })

@bp.post("/gpt/summary")
def gpt_summary():
    sql = """
            ;WITH maxd AS (
            SELECT MAX(TRY_CONVERT(date, [As of Date])) AS max_date
            FROM [NPL].[dbo].[t_insightView_KRI]
        )
        SELECT
            t.[KRI ID]                AS kriId,
            t.[KRI_Name]              AS kriName,
            t.[Adjusted Current Mth]  AS adjustedCurrentMth,
            t.[Risk Type]             AS riskType,
            t.[ImpactBin_Col],
            t.[LikelihoodBin_Col],
            t.[ExposureScoreCol]      AS exposureScore,
            t.[KRI Status]            AS kriStatus,
            TRY_CONVERT(date, t.[As of Date]) AS asOfDate,
            CASE WHEN t.[KRI Status] = 'Breached' THEN 1 ELSE 0 END AS isBreached,
            CASE WHEN t.[KRI Status] = 'Warning'  THEN 1 ELSE 0 END AS isWarning,
            CASE WHEN t.[ImpactBin_Col] = 3 AND t.[LikelihoodBin_Col] = 5 THEN 1 ELSE 0 END AS highImpactHighLikelihood
        FROM [NPL].[dbo].[t_insightView_KRI] t
        CROSS JOIN maxd
        WHERE TRY_CONVERT(date, t.[As of Date]) = maxd.max_date
        AND t.[TOP_KRIs] = 1
        ORDER BY t.[Risk Type], t.[KRI_Name];
    """
    
    rows = run_query(sql)
    compact = {"source": "KRI", "window": "current_month", "rows": rows}
    summary_payload = {
        "messages": [
            {"role": "system", "content": summary_prompt},
            {"role": "user", "content": json.dumps(compact, ensure_ascii=False)}
        ]
    }

    summary_text = call_gpt(summary_payload)
    as_of_date = rows[0]["asOfDate"]
    insert_summary(summary_text, "KRI", as_of_date)
    dashboard_name = None
    dashboard_link = None
    link_info = get_published_address("KRI")
    print("📊 Dashboard link info:", link_info)
    
    email_body = summary_text 
    if link_info:
        dashboard_name, dashboard_link = link_info
        email_body += f"""
            <br><br>
            <div style="text-align:center; margin-top:25px;">
                <a href="{dashboard_link}"
                style="background-color:#0078D4; color:#fff; padding:12px 24px;
                        text-decoration:none; border-radius:6px; font-weight:bold;
                        font-family:Segoe UI, sans-serif;">
                    🔗 View {dashboard_name}
                </a>
            </div>
            <p style="text-align:center; font-size:12px; color:#555; margin-top:10px;">
                If the button above doesn’t work, copy and paste this link:<br>
                <a href="{dashboard_link}" style="color:#0078D4;">{dashboard_link}</a>
            </p>
        """
    else:
            email_body += """
            <br><br>
            <p style="text-align:center; color:#999;">
                Dashboard link currently unavailable.
            </p>
        """
    recipients = [
    "g.agyeabour@awcghana.com",
    "m.williams@awcghana.com",
    "patrick@awcghana.com",
    "s.namoafo@awcghana.com",
    "b.oagyemang@awcghana.com",
    ]
    subject = f"DBG KRI Summary – {as_of_date}"

    success = send_summary_email(subject, email_body, recipients, is_html=True)

    if success:
        # Mark as emailed
        update_sql = """
            UPDATE [NPL].[dbo].[t_insightView_Email_Summaries]
            SET IsEmailed = 1
            WHERE SummaryType = 'KRI' AND AsOfDate = :asOfDate;
        """
        with ENGINE.begin() as conn:
            conn.execute(text(update_sql), {"asOfDate": as_of_date})

        print(f"✅ Summary for {as_of_date} emailed successfully.")
    else:
        print(f"⚠️ Email failed for summary {as_of_date}. Will remain IsEmailed = 0.")

    return jsonify({
        "summary_saved": True,
        "emailed": success,
        "asOfDate": as_of_date,
        "summary": summary_text
    })
   
