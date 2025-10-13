# Recruiter LLM Mailer

Pulls a Google Sheet (published as CSV), drafts **company+recruiter specific** emails with OpenAI, attaches `resume.pdf`, and sends via SMTP. Idempotent via `sent_log.json`. Optional GitHub Actions runs every 2 hours.

## Quick start

1) Clone & install
```bash
git clone <your-repo-url> && cd recruiter-llm-mailer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
