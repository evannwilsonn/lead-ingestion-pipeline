# Lead Ingestion Pipeline — Setup

Gmail ("New Website Inquiry") → Claude (structured extraction + tiering) → Airtable upsert.

## 1. Google Cloud Console (Gmail API credentials)

1. Go to https://console.cloud.google.com and create a new project (e.g. `lead-pipeline`).
2. **APIs & Services → Library**, search **Gmail API**, click **Enable**.
3. **Google Auth Platform → Branding**: set an app name and support email.
4. **Audience**:
   - Google Workspace account: choose **Internal**. No verification needed and tokens don't expire.
   - Personal Gmail: choose **External**, add your Gmail address under **Test users**, then click **Publish app** to move it to *In production*. Apps left in *Testing* get refresh tokens that expire after 7 days, which would silently break the cron job. You'll see an "unverified app" warning during sign-in; click *Advanced → Go to app* (fine for your own account).
5. **Data Access → Add or remove scopes**: add `https://www.googleapis.com/auth/gmail.modify` (needed to read mail, mark it read, and apply labels).
6. **Clients → Create client → Desktop app**. Download the JSON and save it as `client_secret.json`.
7. On your laptop (needs a browser):
   ```bash
   python3 -m venv venv && source venv/bin/activate
   pip install -r requirements.txt
   export GMAIL_CLIENT_SECRET_FILE=./client_secret.json GMAIL_TOKEN_FILE=./token.json
   python lead_pipeline.py --authorize
   ```
   Approve access in the browser. This writes `token.json` (contains a refresh token — treat it like a password).

## 2. Airtable

Create a table (e.g. `Leads`) with these exact field names:

| Field | Type |
|---|---|
| First Name | Single line text |
| Last Name | Single line text |
| Email | Email |
| Phone | Phone number |
| Estimated Budget | Currency |
| Lead Tier | Single select (Tier 1, Tier 2, Tier 3) |
| Summary | Long text |
| Gmail Message ID | Single line text |
| Received At | Date (include time) |

Create a personal access token at https://airtable.com/create/tokens with scopes `data.records:read` and `data.records:write`, and grant it access to this base. The base ID is the `app...` segment of the base URL.

Upserts merge on **Email** (a repeat inquiry updates the existing row with the latest summary/tier) or, if no email was found, on **Gmail Message ID** so reruns never duplicate.

## 3. Anthropic

Create an API key at https://console.anthropic.com. Override the model with `CLAUDE_MODEL` if needed.

## 4. Deploy to a cloud server (Ubuntu example)

```bash
sudo useradd --system --create-home --home-dir /opt/lead-pipeline leadbot
sudo -u leadbot -i
cd /opt/lead-pipeline
# copy lead_pipeline.py, requirements.txt, run_pipeline.sh, .env.example here
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
mkdir -p secrets && chmod 700 secrets
cp .env.example .env && chmod 600 .env   # fill in values
chmod +x run_pipeline.sh
```

From your laptop, copy the token up and lock it down:

```bash
scp token.json you@server:/tmp/ && ssh you@server \
  'sudo mv /tmp/token.json /opt/lead-pipeline/secrets/ && sudo chown leadbot: /opt/lead-pipeline/secrets/token.json && sudo chmod 600 /opt/lead-pipeline/secrets/token.json'
```

Test before scheduling (prints parsed JSON, writes nothing):

```bash
./run_pipeline.sh --dry-run
./run_pipeline.sh            # one real run
```

## 5. Schedule with cron

```bash
sudo mkdir -p /var/log/lead-pipeline && sudo chown leadbot: /var/log/lead-pipeline
sudo -u leadbot crontab -e
```

Add (every 5 minutes):

```
*/5 * * * * /opt/lead-pipeline/run_pipeline.sh >> /var/log/lead-pipeline/run.log 2>&1
```

`flock` in the wrapper skips a run if the previous one is still going. Rotate logs with `/etc/logrotate.d/lead-pipeline`:

```
/var/log/lead-pipeline/*.log {
    weekly
    rotate 8
    compress
    missingok
    notifempty
    copytruncate
}
```

(A systemd timer works equally well if you prefer `journalctl` logging.)

## How failures are handled

- **Transient** (network drops, 429s, 5xx, Claude overloaded): retried with backoff inside the run; if still failing, the email is left unread and picked up next run. Three in a row aborts the run early.
- **Permanent** (empty body, Airtable schema mismatch, unparseable output): email gets the `Lead-Failed` label and stays unread for manual review; it's excluded from future searches. Remove the label to retry.
- **Fatal** (bad/missing keys, revoked Gmail token, wrong base ID): run exits with code 2. Watch for `CRITICAL` in the log; a revoked token means re-running `--authorize`.
- Missing lead fields are stored as blank rather than guessed, and are never written over existing Airtable values.

For stricter secret handling, inject the same env vars from your cloud's secret manager (AWS Secrets Manager, GCP Secret Manager) instead of a `.env` file.
