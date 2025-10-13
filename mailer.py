#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Mailer: recruiter outreach with LLM-generated copy + resume attachment.

Highlights in this version:
- Uses gpt-4o-mini (cheap/fast) and strips ```json fences before parsing
- Falls back to a concise hand-written template if LLM fails or TEST_MODE=true
- Adds simple CAN-SPAM footer + List-Unsubscribe header for deliverability
- Uses timezone-aware UTC timestamps (no deprecation warning)
- DRY_RUN support and local idempotency via sent_log.json
"""

import os, ssl, smtplib, json, time, datetime, io, re
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from email import encoders

import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ---------- Load env ----------
load_dotenv()

# ---- Config from env ----
SHEET_CSV_URL   = os.getenv("SHEET_CSV_URL", "").strip()
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "").strip()

SENDER_NAME     = os.getenv("SENDER_NAME", "Aniket Patole").strip()
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "").strip()

SMTP_HOST       = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT       = int(os.getenv("SMTP_PORT", "465"))
SMTP_USERNAME   = os.getenv("SMTP_USERNAME", SENDER_EMAIL).strip()
SMTP_PASSWORD   = os.getenv("SMTP_PASSWORD", "").strip()

MAX_PER_RUN         = int(os.getenv("MAX_PER_RUN", "10"))
RATE_LIMIT_SECONDS  = int(os.getenv("RATE_LIMIT_SECONDS", "8"))
DRY_RUN             = os.getenv("DRY_RUN", "false").lower() == "true"
TEST_MODE           = os.getenv("TEST_MODE", "false").lower() == "true"  # bypass LLM; send template

# Optional: helps clients unsubscribe/reply-stop (deliverability + compliance)
LIST_UNSUBSCRIBE_MAILTO = os.getenv("LIST_UNSUBSCRIBE_MAILTO", SENDER_EMAIL).strip()  # mailto: address used in header

SENT_LOG_PATH   = Path("sent_log.json")
RESUME_PATH     = Path("Resume_Aniket_Patole.pdf")

# ---- OpenAI client (v1 SDK) ----
# If TEST_MODE or no key, we'll skip importing client to avoid confusion.
client = None
if OPENAI_API_KEY and not TEST_MODE:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

# ---------- Prompts ----------
SYSTEM_PROMPT = """You are a concise outreach assistant.
Write a professional cold email to a recruiter to open a conversation for roles based in the US for Data Analyst / Data Engineer roles.
Tone: warm, direct, under 250 words. No fluff.
One paragraph + up to 3 compact bullets with quantified wins based on my resume below. One bullet should mention my Accenture experience section.

Resume (facts for reference only; do not dump verbatim):
Aniket Patole — Data Engineer with 3+ years’ experience designing fault-tolerant ETL, data warehouses, and scalable data architecture (AWS S3/Lambda/Batch/Athena/Redshift; GCP; Databricks). SQL, Python, Spark, dbt, Airflow, HiveQL, Scala. Data modeling, ad-hoc analysis, BI (Tableau/Power BI/QuickSight). Fraud analytics, healthcare reporting, cloud migrations. AWS Data Engineer – Associate and Databricks Data Engineer – Associate (2025).
- Completed Masters in Information Systems (Data Science) from Northeastern University, Boston, MA.
- Completed Bachelors in Computer Engineering from Pune University, India.
- Accenture (Royal Mail Group): 1M+ weekly parcel transactions; fraud & SLA insights across 10+ sources; compliance and master data syncs; mentoring SQL optimization.
- Northeastern Univ as a Data Analyst for the admissions team: ETL across Salesforce/Slate/Five9; reduced reporting TAT 3 days→1; dimensional models; Tableau for 8k+ applicants; anomaly detection +20% data quality.


DO:
- Personalize to the recruiter/company.
- Keep it plainly formatted (no code blocks), with a clear subject and body.
- Return strict JSON with keys: "subject", "body" (no extra keys, no markdown fences).
DON'T:
- Don’t include backticks, markdown fences, or code formatting.
- Don’t exceed ~250 words in the body.
- Don’t sell too hard.
Sign as: Aniket Patole.
"""

USER_PROMPT = """Context:
- Candidate: Data Analyst/Engineer (SQL, Python, Spark, AWS, ETL, BI).
- Target company: {company}
- Recruiter name: {full_name}
- Optional notes/personalization: {notes}

Requirements:
- Subject line: 5–7 words, specific to {company}.
- Body: 1 paragraph + up to 3 bullets with quantified wins.
- Include a contact line with phone + links placeholder.
DO NOT include any signature, phone, email, or address.
Return only the subject and body content (body must not include a closing/signature).
Return JSON ONLY with keys: subject, body.
"""

# ---------- Helpers ----------
def fetch_sheet_df(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def strip_markdown_fences(text: str) -> str:
    """Remove ```json ... ``` or ``` ... ``` fences if present."""
    if text.strip().startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text.strip())
        text = re.sub(r"\s*```$", "", text.strip())
    return text.strip()


# --- Encrypted sent_log using Fernet ---
import base64
from cryptography.fernet import Fernet

SENT_LOG_KEY = os.getenv("SENT_LOG_KEY", "").encode()  # 32-byte urlsafe base64 key
SENT_LOG_ENC_PATH = Path("sent_log.enc")

def _get_fernet():
    if not SENT_LOG_KEY:
        raise SystemExit("Missing SENT_LOG_KEY in env (use a 32-byte urlsafe base64 key from Fernet.generate_key())")
    return Fernet(SENT_LOG_KEY)

def load_sent_log():
    """Load encrypted sent_log if present; otherwise return empty structure."""
    if not SENT_LOG_ENC_PATH.exists():
        return {"emails": {}}
    f = _get_fernet()
    data = SENT_LOG_ENC_PATH.read_bytes()
    try:
        decrypted = f.decrypt(data)
        return json.loads(decrypted.decode("utf-8"))
    except Exception:
        # if anything goes wrong, start fresh rather than crash
        return {"emails": {}}

def save_sent_log(log: dict):
    f = _get_fernet()
    blob = f.encrypt(json.dumps(log, indent=2).encode("utf-8"))
    SENT_LOG_ENC_PATH.write_bytes(blob)


def fallback_template(full_name: str, company: str) -> tuple[str, str]:
    subj = f"Exploring data roles at {company or 'your team'}"
    body = (
        f"Hi {full_name},\n\n"
        "I’m a Data Engineer (SQL, Python, Spark, AWS, dbt/Airflow) with 3+ years building fault-tolerant ETL and BI data models. "
        f"I’m interested in opportunities at {company or 'your team'} and would love to be considered.\n\n"
        "• Reduced reporting turnaround from 3 days → 1 via Salesforce/Slate/ETL automation\n"
        "• Built 5M+/day streaming pipeline; fraud/SLA analytics on 1M+ weekly parcels\n\n"
        "Open to a quick 10-minute chat?\n\n"
        "Best,\n"
        "Aniket Patole\n"
        "(617)-352-5273 | LinkedIn | Portfolio"
    )
    return subj, body
import re

PLACEHOLDER_RE = re.compile(r"\[.*?\]")  # [Your Phone Number], [Your LinkedIn Profile], etc.

def name_from_email(email: str) -> str:
    local = (email or "").split("@")[0]
    # john.doe_smith -> "John Doe Smith"
    local = local.replace(".", " ").replace("_", " ")
    # Collapse multiple spaces and title case
    nice = re.sub(r"\s+", " ", local).strip().title()
    # Prefer first token (Hi John,) to avoid weird long greetings
    return nice.split(" ")[0]

def strip_existing_signature(body: str) -> str:
    """
    Remove any LLM-added signature blocks and placeholders.
    Heuristics:
      - Remove from a trailing salutation like: Best, Regards, Sincerely, Thanks
      - Remove common placeholder lines in brackets [ ... ]
    """
    if not body:
        return body

    lines = body.strip().splitlines()
    cut_idx = len(lines)
    sig_triggers = (
        "best,", "regards,", "thanks,", "thank you,", "sincerely,", "cheers,", "--"
    )

    for i, line in enumerate(lines):
        L = line.strip().lower()
        if L in sig_triggers or any(L.startswith(t) for t in sig_triggers):
            cut_idx = i
            break

    clean = "\n".join(lines[:cut_idx])
    # Remove bracket placeholders anywhere that slipped in
    clean = PLACEHOLDER_RE.sub("", clean)
    # Collapse 3+ newlines to max 2
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    return clean

def canon_signature_and_footer() -> str:
    signature = (
        "\n\nBest,\n"
        "Aniket Patole\n"
        "(617) 352-5273 | LinkedIn | Portfolio"
    )
    footer = (
        "\n\n--\n"
        "Aniket Patole • 75 Saint Alphonsus Street, Apt 1816, Boston, MA 02120 • USA\n"
        "If this isn’t relevant, reply “stop” and I’ll remove you."
    )
    return signature + footer


import re
import json

GREETING_LINE_RE = re.compile(
    r'^\s*(hi|hello|hey|good\s+(morning|afternoon|evening))\b[^\n]*\n?',
    flags=re.IGNORECASE
)

def strip_leading_greeting(body: str) -> str:
    """Remove a leading greeting line like 'Hi there,' or 'Hello John,' if present."""
    if not body:
        return body
    return GREETING_LINE_RE.sub("", body, count=1).lstrip()


def generate_email(full_name: str, company: str, notes: str = "") -> tuple[str, str]:
    """Generate (subject, body) via LLM with robust JSON parsing.
       Always greets with full_name and forces a fixed first bullet.
    """
    def _safe_parse_json(txt: str) -> dict:
        txt = strip_markdown_fences(txt or "")
        try:
            return json.loads(txt)
        except Exception:
            # salvage {...} block if model added extra text
            m = re.search(r"\{.*\}", txt, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(0))
                except Exception:
                    return {}
            return {}

    if TEST_MODE or client is None:
        subject, body = fallback_template(full_name, company)
    else:
        prompt = USER_PROMPT.format(full_name=full_name or "", company=company or "", notes=notes or "")
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.6,
            )
            text = resp.choices[0].message.content or ""
            data = _safe_parse_json(text)
            subject = (data.get("subject") or f"Exploring data roles at {company or 'your team'}").strip()
            body = (data.get("body") or "").strip()
            if not body:
                # fall back if body missing
                subject, body = fallback_template(full_name, company)
        except Exception as e:
            print(f"[LLM fallback] Reason: {e}")
            subject, body = fallback_template(full_name, company)

    # ---- sanitize body text ----
    body = strip_markdown_fences(body)
    body = strip_existing_signature(body)
    body = strip_leading_greeting(body)

    # ---- enforce greeting with sheet name ----
    first_name = (full_name or "").split()[0].strip() or "there"
    greeting = f"Hi {first_name},\n\nI know your time is valuable, so I'll keep this brief:\n\n"

    # ---- fixed first bullet ----
    fixed_intro_bullet = (
        "- I'm Aniket, Data Engineer/Analyst with 3+ years’ professional experience, "
        "recently graduated with a Master’s in Information Systems from Northeastern University, Boston MA."
    )

    # Collect bullets from LLM body (lines starting with "- ")
    lines = [ln.rstrip() for ln in body.splitlines() if ln.strip()]
    bullets = [ln for ln in lines if ln.lstrip().startswith("- ")]

    # If no bullets, derive 1–2 bullets from sentences
    if not bullets:
        # take up to first 2 sentences and make bullets
        sentences = re.split(r"(?<=[.!?])\s+", body)
        sentences = [s.strip() for s in sentences if s.strip()]
        derived = [f"- {s}" for s in sentences[:2]]
        bullets = derived or ["- Experienced building ETL pipelines and analytics-ready data models.",
                              "- Comfortable with SQL, Python, Spark, AWS, dbt/Airflow, and BI tooling."]

    # Force fixed first bullet
    if bullets:
        bullets[0] = fixed_intro_bullet
    else:
        bullets = [fixed_intro_bullet]

    # ---- closing ----
    closing = (
        f"\n\nI would love to discuss potential opportunities and how my skills can align with {company}'s goals. "
        "Please let me know a suitable time to connect."
    )

    # ---- assemble plain-text body ----
    body_final = greeting + "\n".join(bullets) + closing

    # ---- signature ----
    signature = (
        "\n\nBest,\n"
        "Aniket Patole\n"
        "(617) 352-5273\n"
        "LinkedIn: https://www.linkedin.com/in/aniketpatole/\n"
        "Portfolio: https://aniketpatole.github.io/"
    )
    body_final = body_final.rstrip() + signature

    return subject, body_final





import re
import html
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formataddr, formatdate, make_msgid

URL_RE = re.compile(r"(https?://[^\s)]+)")

def plain_to_html(body_plain: str) -> str:
    """Convert your plain-text body to simple, clean HTML:
       - paragraphs
       - real bullet list for lines starting with '- '
       - clickable links
    """
    body_plain = body_plain or ""
    # Escape HTML entities
    text = html.escape(body_plain)

    # Linkify raw URLs
    def _linkify(m):
        url = m.group(1)
        escaped = html.escape(url)
        return f'<a href="{escaped}">{escaped}</a>'
    text = URL_RE.sub(_linkify, text)

    # Split into paragraphs and detect bullet blocks
    lines = text.splitlines()
    html_lines = []
    in_list = False

    for line in lines:
        s = line.strip()
        if s.startswith("- "):  # bullet line
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            item = s[2:].strip()
            html_lines.append(f"<li>{item}</li>")
        else:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if s == "":
                html_lines.append("<p></p>")
            else:
                html_lines.append(f"<p>{s}</p>")

    if in_list:
        html_lines.append("</ul>")

    # Minimal wrapper (no external CSS)
    html_body = (
        "<!doctype html><html><body>"
        + "\n".join(html_lines)
        + "</body></html>"
    )
    return html_body


def send_email_with_attachment(to_name: str, to_email: str, subject: str, body_plain: str):
    # Defensive subject cleanup (no CRLF injection)
    subject = (subject or "").replace("\r", " ").replace("\n", " ").strip()

    # ---- Outer container: mixed (for alt + attachment) ----
    msg = MIMEMultipart("mixed")
    msg["Subject"]    = subject
    msg["From"]       = formataddr((SENDER_NAME, SENDER_EMAIL))
    msg["To"]         = formataddr((to_name, to_email))
    msg["Reply-To"]   = SENDER_EMAIL
    msg["Date"]       = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()

    if LIST_UNSUBSCRIBE_MAILTO:
        msg.add_header("List-Unsubscribe", f"<mailto:{LIST_UNSUBSCRIBE_MAILTO}>")
        msg.add_header("List-Unsubscribe-Post", "List-Unsubscribe=One-Click")

    # ---- Inner container: alternative (plain + html) ----
    alt = MIMEMultipart("alternative")

    # Plain-text part (what spam filters/legacy clients read)
    text_part = MIMEText(body_plain or "", "plain", "utf-8")
    alt.attach(text_part)

    # HTML part (nice rendering with clickable links)
    html_body = plain_to_html(body_plain or "")
    html_part = MIMEText(html_body, "html", "utf-8")
    alt.attach(html_part)

    # Attach the alternative block to the mixed container
    msg.attach(alt)

    # ---- Attachment: resume.pdf (optional) ----
    if RESUME_PATH.exists():
        with open(RESUME_PATH, "rb") as f:
            part = MIMEApplication(f.read(), _subtype="pdf")
            part.add_header("Content-Disposition", f'attachment; filename="{RESUME_PATH.name}"')
            msg.attach(part)

    if DRY_RUN:
        print(f"[DRY_RUN] Would send to {to_email}: {subject}")
        print("--- Plain preview ---")
        print("\n".join((body_plain or "").splitlines()[:12]))
        return

    # ---- Send ----
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SENDER_EMAIL, [to_email], msg.as_string())



def main():
    if not SHEET_CSV_URL:
        raise SystemExit("Missing SHEET_CSV_URL in env")
    if not SENDER_EMAIL or not SMTP_HOST or not SMTP_PASSWORD:
        raise SystemExit("Missing SMTP config in env (SENDER_EMAIL, SMTP_HOST, SMTP_PASSWORD).")

    # --- Read rows from published CSV
    df = fetch_sheet_df(SHEET_CSV_URL)

    required = {"email", "company"}
    if not required.issubset(df.columns):
        raise SystemExit(f"Your sheet must contain columns: {required}. Found: {df.columns.tolist()}")

    # optional personalization columns
    for opt in ["full_name", "status", "notes"]:
        if opt not in df.columns:
            df[opt] = ""

    sent_log = load_sent_log()
    already_sent = set((sent_log.get("emails") or {}).keys())

    # Select candidates to send
    to_send = []
    for _, row in df.iterrows():
        email = str(row["email"]).strip()
        if not email:
            continue
        status = str(row.get("status", "")).strip().lower()
        if status == "sent":
            continue
        if email in already_sent:
            continue
        to_send.append(row)

    # Respect cap
    to_send = to_send[:MAX_PER_RUN]
    print(f"Found {len(to_send)} recipient(s) to email.")

    for row in to_send:
        email    = str(row["email"]).strip()
        full_raw = str(row.get("full_name", "")).strip()
        full     = full_raw if full_raw else name_from_email(email)
        company  = str(row.get("company", "")).strip()
        notes    = str(row.get("notes", "")).strip()

        subject, body = generate_email(full, company, notes)

        try:
            send_email_with_attachment(full, email, subject, body)

            iso_ts = datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")
            sent_log.setdefault("emails", {})[email] = {
                "full_name": full,
                "company": company,
                "subject": subject,
                "sent_at": iso_ts
            }
            save_sent_log(sent_log)
            print(f"✓ Sent: {full} <{email}> | {company} | {subject}")
            time.sleep(RATE_LIMIT_SECONDS)

        except Exception as e:
            print(f"✗ Failed: {email} — {e}")

if __name__ == "__main__":
    main()
