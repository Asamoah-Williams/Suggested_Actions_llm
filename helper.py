# helper.py
import os, json, requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import date, datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import quote_plus
import threading
import time
import requests
load_dotenv()

SQL_SERVER   = os.getenv("SQL_SERVER", "192.168.10.204")
SQL_DB       = os.getenv("SQL_DB", "NPL")
SQL_USERNAME = os.getenv("SQL_USERNAME", "awc_sql")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "AwC@2023")
SQL_DRIVER   = os.getenv("SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY","")
OPENAI_BASE    = os.getenv("OPENAI_BASE","https://api.openai.com/v1")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL","gpt-4o-mini")
OWNER_ALLOW    = {d.strip().lower() for d in os.getenv("OWNER_ALLOW_DOMAINS","awcghana.com").split(",")}

odbc_str = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DB};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD};"
    "Encrypt=Optional;"
    "TrustServerCertificate=Yes;"
)

ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}", fast_executemany=True)

summary_prompt = """
You are a Senior Risk Analyst at the Development Bank of Ghana (DBG), a wholesale development finance institution that channels funding to MSMEs through Participating Financial Institutions (PFIs).

You are preparing the **Monthly Executive Risk Summary Report** for DBG Management.

Input data:
You will receive a compact JSON array containing detailed Key Risk Indicator (KRI) data per risk type, including fields such as:
- kriId
- kriName
- riskType
- riskLevel (derived from impact and likelihood ratings; reflects severity)
- kriStatus (current state: Breached, Warning, or Safe)
- adjustedCurrentMth (the latest measured value)
- avgExposure
- asOfDate (reporting date)

Interpretation:
- `riskLevel` represents potential severity: KRIs with impact ≥ 3 and likelihood ≥ 5 are considered High severity, even if not yet breached.
- `kriStatus` indicates the current risk position: Breached or Warning. Ignore all records marked as Safe or Low risk.
- Focus on active or emerging risks — those marked as Breached, Warning, or High/Moderate in overall risk level.

Your task:
Write a concise, data-driven, professional report (approximately six paragraphs) that:
1. Clearly states the **reporting month and year** based on the `asOfDate` field.
2. Summarizes the overall risk environment — indicating whether DBG’s risk profile is improving, stable, or worsening relative to prior months.
3. Highlights the main **risk types and KRIs** showing breaches or warnings, identifying patterns or concentrations.
4. Discusses **high-severity (riskLevel = High/Moderate)** indicators that may not yet be breached but represent material potential risk exposures.
5. Explains how these emerging risks could affect DBG’s portfolio, PFIs, or regulatory compliance if not managed proactively.
6. Concludes with **forward-looking insights** and proposed management focus areas, consistent with DBG’s Basel III and Bank of Ghana-aligned governance standards.

DBG Context (for tone and realism):
- DBG operates under Basel III and Bank of Ghana regulations.
- The institution’s core teams include Risk, Treasury, Finance, Legal/Compliance, ESG, Monitoring & Evaluation (M&E), and Wholesale Lending.
- Summaries must maintain a **neutral, factual, and analytical tone** suitable for internal board and executive-level reporting.

Formatting:
- Write in formal report prose — no bullet points, no lists, no markdown.
- Include the reporting date in the opening line, e.g., “As of May 2025, DBG’s overall risk profile...”
- Output plain text only — no JSON, no markup, no headings.

Example opening:
“As of May 2025, DBG’s overall risk profile reflected a moderate elevation in portfolio-level exposure, with heightened sensitivities in credit and liquidity risk categories. Breaches were concentrated in operational metrics, while several high-severity indicators signaled potential vulnerabilities requiring proactive oversight.”

Your goal is to produce a **polished, regulatory-aligned, and forward-looking** summary that integrates both current risk positions and potential high-severity exposures, reflecting DBG’s professional reporting standards.
"""


SYSTEM_PROMPT = """
You are a seasoned Risk Analyst consultant for the Development Bank of Ghana (DBG), 
a wholesale lending institution that provides funding to MSMEs through Participating Financial Institutions (PFIs).

You will receive compact JSON rows describing Key Risk Indicators (KRIs) that have breached or are at warning levels. 
Your task is to generate *specific, actionable, regulatory-compliant, and essay-style recommendations* for risk mitigation. 

DBG Context:
- DBG is structured with multiple specialized teams: Legal/Compliance, Risk, Finance, Treasury, Wholesale Lending, Investor Relations, ESG, Monitoring & Evaluation (M&E), HR, and Marketing.
- Each team already exists and is functional. **Do not recommend creating new teams or roles.**
- Recommendations should direct the **appropriate existing team(s)** to take targeted actions.
- DBG adheres to **Basel III standards** and **Bank of Ghana regulations**, which must guide all mitigation and governance advice.
- When coordinating actions across teams, refer to existing collaboration mechanisms (e.g., joint review sessions, escalation meetings, or cross-functional working groups), not “new task forces” or “new committees.”

Tone and Style:
- Write in a professional, neutral, and advisory tone suitable for executive summaries and Power BI dashboards.

- Use clear, concise, and formal English — avoid conversational or overly narrative phrasing.
- Frame recommendations as **forward-looking actions** (what DBG *will do* or *should do*), not as retrospective commentary.

Guidelines:
1. Base recommendations on **tested and proven tactics** that have been successfully applied in Ghana’s wholesale banking and MSME financing sector.
2. Ensure all suggestions comply with **Bank of Ghana** and **Basel III** regulatory frameworks, as well as DBG’s own internal governance and risk management structures.
3. The `KRI_Name` (metricName) and `riskType` fields are the main drivers of your analysis. Use them to:
   - Identify the **appropriate DBG team(s)** to take action (e.g., Legal/Compliance for regulatory breaches, Treasury for liquidity risks, Finance for reporting or capital issues, ESG for sustainability, etc.).
   - Tailor the recommended actions to that team’s actual role and capacity within DBG.
   - Be flexible: if the `metricName` implies another functional owner, adjust accordingly rather than following a fixed mapping.
4. Each recommendation must clearly specify **which DBG team(s)** should take ownership of the mitigation actions, 
and describe what those teams should *realistically do* within their mandate — e.g., investigation, adjustment, monitoring, reporting, or corrective action — 
without restating obvious responsibilities or suggesting new teams.

5. Recommendations must be written in concise **essay format (2–3 paragraphs max)**:
   - Explain the issue clearly in context.
   - Outline its implications for DBG and PFIs.
   - Provide a realistic and pragmatic mitigation plan, assigning clear responsibility to one or more DBG teams.
6. Recommendations should aim to **reduce or eliminate the breach**, targeting a return of the KRI status to "Safe" (0).
7. Be pragmatic and realistic: suggest actions that DBG and its PFIs *can actually implement* (e.g., enhanced monitoring, borrower due diligence, liquidity buffer reinforcement, ESG reporting, legal compliance checks).
8. When suggesting a `postMitigationValue`, base it on the **threshold, warning, and escalation limits/operators** provided in the KRI row.
   - Respect the operators (`<`, `<=`, `>`, `>=`) exactly as defined in the KRI input.
   - If you are not confident about the appropriate post-mitigation value, leave `postMitigationValue` as `null`.

Output rules:
- Output strictly in JSON format — no explanations, no prose outside of JSON, no markdown.
- For every row in the input "rows" array, output exactly one recommendation object.
- Do not skip rows and do not merge multiple rows into one recommendation.
- Each recommendation must carry over `relatedEntityId`, `metricName`, `metricValue`, `observedAt`, and `riskType` exactly as provided.
- The number of recommendations in your output must equal the number of input rows.

Output fields per recommendation:
- source  
- relatedEntityId  
- metricName  
- metricValue  
- recommendationText (essay style, 2-3 paragraphs with justification + actions)  
- actionType (EmailStakeholders|RaiseStock|SlackNotify|Investigate|NoAction)  
- confidence (0..1)  
- referenceTimestamp (ISO-8601)  
- observedAt (ISO-8601, from the KRI record)  
- riskType  
- metadata (object, {} if none)  
- postMitigationValue (float or null)  
"""



def run_query(sql: str, params: dict | None = None) -> list[dict]:
    with ENGINE.begin() as conn:
        res = conn.execute(text(sql), params or {})
        cols = res.keys()
        rows = []
        for row in res.fetchall():
            row_dict = {}
            for col, val in zip(cols, row):
                if isinstance(val, (date, datetime)):
                    row_dict[col] = val.isoformat()
                else:
                    row_dict[col] = val
            rows.append(row_dict)
        return rows


def normalize_record(r: dict) -> dict:
    rec = dict(r)
    # ReferenceTimestamp → datetime
    if rec.get("referenceTimestamp"):
        try:
            rec["referenceTimestamp"] = datetime.fromisoformat(
                rec["referenceTimestamp"].replace("Z", "")
            )
        except Exception:
            rec["referenceTimestamp"] = None
    # ObservedAt → date
    if rec.get("observedAt"):
        try:
            rec["observedAt"] = datetime.fromisoformat(
                str(rec["observedAt"]).replace("Z", "")
            ).date()
        except Exception:
            try:
                rec["observedAt"] = date.fromisoformat(str(rec["observedAt"]))
            except Exception:
                print(f"[WARN] Could not parse observedAt: {rec['observedAt']}, defaulting to today")
                rec["observedAt"] = date.today()

    # Metadata → JSON string
    if isinstance(rec.get("metadata"), dict):
        rec["metadata"] = json.dumps(rec["metadata"], ensure_ascii=False)
    return rec

def is_latest_kri_processed() -> bool:
    """
    Returns True if the latest KRI 'As of Date' already exists in Recommendations.
    """
    sql = """
    ;WITH kri_max AS (
        SELECT MAX(TRY_CONVERT(date, [As of Date])) AS kri_latest
        FROM [NPL].[dbo].[t_insightView_KRI]
    ),
    rec_check AS (
        SELECT MAX(TRY_CONVERT(date, ObservedAt)) AS rec_latest
        FROM [NPL].[dbo].[t_insightView_Recommendations]
    )
    SELECT
        CASE
            WHEN kri_latest <= ISNULL(rec_latest, '1900-01-01') THEN 1
            ELSE 0
        END AS is_processed,
        kri_latest, rec_latest
    FROM kri_max CROSS JOIN rec_check;
    """
    result = run_query(sql)
    if result:
        row = result[0]
        print(f"Latest KRI date: {row['kri_latest']}, Latest Recommendation: {row['rec_latest']}")
        return row["is_processed"] == 1
    return False



def send_summary_email(subject: str, body: str, recipients: list[str],is_html: bool = True) -> bool:
    """
    Sends an email using Office 365 SMTP settings.
    Returns True if successful, False otherwise.
    """
    try:
        SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
        SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
        SMTP_EMAIL = os.getenv("SMTP_EMAIL")
        SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

        msg = MIMEMultipart()
        msg["From"] = SMTP_EMAIL
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject

        
        msg.attach(MIMEText("plain"))
        if is_html:
            # wrap in minimal HTML for safe rendering
            html_body = f"<html><body>{body}</body></html>"
            msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.send_message(msg)

        print(f"📧 Email sent to: {', '.join(recipients)}")
        return True

    except Exception as e:
        print(f"[EMAIL ERROR] {e}")
        return False

def get_published_address(summary_type: str) -> tuple[str, str] | None:
    """
    Returns (DashboardName, publishedAddress) for the given summary type.
    Uses the t_PublishedDashboards table.
    """
    mapping = {
        "KRI": "Key Risk Indicator Overview",
        "Finance": "Financial Overview",
        "ESG": "ESG Dashboard",
        "Treasury": "Treasury Performance Dashboard"
    }

    dashboard_name = mapping.get(summary_type.capitalize())
    if not dashboard_name:
        return None

    sql = """
        SELECT TOP 1 DashboardName, publishedAddress
        FROM [NPL].[dbo].[t_PublishedDashboards]
        WHERE DashboardName = :dashboard_name;
    """
    rows = run_query(sql, {"dashboard_name": dashboard_name})
    if rows:
        return rows[0]["DashboardName"], rows[0]["publishedAddress"]
    return None

def auto_scheduler():
    """
    Scheduler that runs every Monday at 12:00 PM.
    If the latest KRI date is already processed → sleep.
    If not → trigger GPT generation and summary in app.py.
    """
    last_run_date = None
    while True:
        now = datetime.now()

        if now.weekday() == 0 and now.hour >= 12:  # Monday = 0
            if last_run_date != now.date():
                print("⏰ Monday 12:00 → Checking if new KRI data exists")

                if is_latest_kri_processed():
                    print("✅ Latest KRI data already has recommendations → Going back to sleep.")
                else:
                    print("🧠 New KRI data detected — triggering GPT generation and summary")

                    try:
                        # Run recommendations first
                        requests.post("http://127.0.0.1:8080/gpt/run", timeout=600)
                        print("✅ Recommendations generated successfully")

                        # Then generate summary
                        requests.post("http://127.0.0.1:8080/gpt/summary", timeout=300)
                        print("✅ Monthly summary generated successfully")

                    except Exception as e:
                        print(f"[GPT RUN ERROR] {e}")

                last_run_date = now.date()
                print("Sleeping for 7 days (until next Monday)...")
                time.sleep(7 * 24 * 3600)

            else:
                time.sleep(3600)
        elif now.weekday() == 0 and now.hour < 12:
            print("Monday but before 12:00 → Sleeping 1 hour")
            time.sleep(3600)
        else:
            print("Not Monday → Sleeping 1 day")
            time.sleep(86400)




def insert_recommendations(recs: list[dict]) -> int:
    """
    Insert a batch of recommendations into dbo.Recommendations.
    Expects each dict in `recs` to have keys that match the placeholders.
    """
    if not recs:
        return 0
    clean_recs = [normalize_record(r) for r in recs]
    sql = text("""
    INSERT INTO dbo.t_insightView_Recommendations (
        Source,
        RelatedEntityId,
        MetricName,
        MetricValue,
        RecommendationText,
        ActionType,
        RiskType,
        Confidence,
        ReferenceTimestamp,
        ObservedAt,
        Metadata,
        PostMitigationValue
    )
    VALUES (
        :source,
        :relatedEntityId,
        :metricName,
        :metricValue,
        :recommendationText,
        :actionType,
        :riskType,               
        :confidence,
        :referenceTimestamp,
        :observedAt,
        :metadata,
        :postMitigationValue
    )
""")

    with ENGINE.begin() as conn:
        conn.execute(sql, clean_recs)

    return len(clean_recs)

def insert_summary(summary_text: str, summary_type: str, as_of_date: str | date) -> int:
    """
    Inserts a GPT-generated summary into dbo.t_insightView_Summaries.
    Automatically marks it as not emailed (IsEmailed = 0).
    Returns the number of inserted rows (should be 1 on success).
    """
    sql = text("""
        INSERT INTO dbo.t_insightView_Email_Summaries (SummaryType, SummaryText, AsOfDate, IsEmailed)
        VALUES (:summaryType, :summaryText, :asOfDate, 0);
    """)

    with ENGINE.begin() as conn:
        conn.execute(sql, {
            "summaryType": summary_type,
            "summaryText": summary_text,
            "asOfDate": as_of_date
        })

    print(f"Summary saved → Type: {summary_type}, Date: {as_of_date}")
    return 1


def owner_ok(email: str) -> bool:
    return email.split("@")[-1].lower() in OWNER_ALLOW

def call_gpt(compact_payload: dict) -> any:
    """
    Handles both recommendation JSON requests and text summaries.
    If the compact_payload has `messages`, treat it as a direct GPT request (used for summaries).
    Otherwise, use SYSTEM_PROMPT + KRI JSON logic.
    """
    url = f"{OPENAI_BASE}/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}

    if "messages" in compact_payload:
        body = {"model": OPENAI_MODEL, **compact_payload, "temperature": 0.1}
    else:
        body = {
            "model": OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(compact_payload, ensure_ascii=False)}
            ],
            "temperature": 0.1
        }
    r = requests.post(url, headers=headers, json=body, timeout=300)
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"].strip()
    if content.startswith("[") or content.startswith("{"):
        try:
            parsed = json.loads(content)
            return parsed if isinstance(parsed, list) else parsed.get("recommendations", [])
        except Exception:
            return []
    else:
        return content



