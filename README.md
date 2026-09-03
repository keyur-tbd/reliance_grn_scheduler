# Reliance GRN Scheduler

Pulls Reliance Retail **GRN** documents out of Gmail every 3 hours, extracts the line items, and writes them to Google Sheets and Supabase.

## Pipeline

```
Gmail  --->  Google Drive  --->  LlamaParse  --->  Google Sheets
                                            \--->  Supabase
```

| Stage | What happens |
|---|---|
| **1. Gmail -> Drive** | Searches mail from `(not filtered by sender)` matching `grn`, looking back 7 days, and saves each attachment to a Drive folder. Already-seen files are skipped. |
| **2. Extract** | Each new file is parsed by **LlamaParse** using the LlamaCloud extract agent `Reliance Agent`. |
| **3. Sheets** | Rows are appended to tab `reliancegrn` of the tracking spreadsheet. |
| **4. Supabase** | `supabase_sink.py --run --source reliance` writes the same rows to table `reliance_grn`. Any field without a typed column is preserved in `raw_data` (jsonb), so a renamed or new field never fails a run. |
| **5. Run log** | A per-run summary is appended to tab `workflow_logs`. |

## Schedule and entry points

Runs on GitHub Actions via `.github/workflows/scheduler.yml`, cron `0 */3 * * *` (every 3 hours). Can also be triggered manually with **Run workflow**.

The workflow deliberately does **not** call `main()`. `app.py` uses the `schedule` library for standalone local use, and that loop would sit idle burning runner minutes. Actions instead invokes the module-level `run_combined_workflow(automation)` after `authenticate()`, then runs the Supabase sink as a separate step.

Recent runs average **~3 minutes**.

## Required secrets

Set under **Settings -> Secrets and variables -> Actions**:

| Secret | Purpose |
|---|---|
| `GOOGLE_CREDENTIALS` | base64 of `credentials.json` (Google OAuth client) |
| `GOOGLE_TOKEN` | base64 of `token.json` (authorized refresh token) |
| `LLAMA_API_KEY` | LlamaCloud key, exported to the app as `LLAMA_CLOUD_API_KEY` |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | service role key - bypasses RLS for writes |

The workflow base64-decodes the two Google secrets into `credentials.json` / `token.json` at the start of the run and deletes them in an `if: always()` cleanup step.

> [!WARNING]
> **`app.py` line 60 contains a hardcoded LlamaCloud API key** as a fallback in the `CONFIG`
> dict (`'llama_api_key': 'llx-...'`). It is redundant - the workflow already supplies
> `LLAMA_CLOUD_API_KEY` from the `LLAMA_API_KEY` secret. Replace the literal with
> `os.getenv('LLAMA_CLOUD_API_KEY', '')`, and rotate the key in LlamaCloud: deleting it
> from the file alone does not help, because it remains in git history.

## Running locally

```bash
pip install -r requirements.txt
# place credentials.json + token.json next to the script
python app.py   # schedule loop; Ctrl-C to stop
```

`supabase_sink.py` is a standalone entry point with its own diagnostics. Run these in order before changing anything in Actions:

```bash
python supabase_sink.py --list-sources          # sources and their tables
python supabase_sink.py --print-schema          # SQL to paste into the Supabase editor
python supabase_sink.py --check                 # config + connectivity + tables exist
python supabase_sink.py --self-test             # insert/read/delete a synthetic row
python supabase_sink.py --run --dry-run --limit 2 --dump-json rows.json
python supabase_sink.py --run --source reliance --limit 2
```

`--source` is passed explicitly in the workflow because `.env` is gitignored and the sink would otherwise fall back to its default source.

> `supabase_sink.py` is **copied verbatim** across the GRN/PRN scheduler repos and holds the registry of every source. A fix here has to be copied to the others to stay in sync.

## Files

| File | Role |
|---|---|
| `app.py` | Gmail -> Drive -> extract -> Sheets, class `RelianceAutomation` |
| `supabase_sink.py` | Drive -> Supabase, shared across scheduler repos |
| `.github/workflows/scheduler.yml` | 3-hourly Actions schedule |
| `requirements.txt` | dependencies |

## Adding a field

The extractor's output reaches Supabase whether or not a typed column exists. To promote a field: extend the `reliance` entry in `SOURCES` in `supabase_sink.py`, run `--print-schema`, and apply the `alter table` it prints. Until then, query it as `raw_data->>'your_key'`.

## Disk guard (shared across every pipeline)

This scheduler writes to a Supabase volume shared with the Business Central
sync and the marketplace/ads loaders. Before it writes, it asks the database
whether it is allowed. **If you get an email titled `[WARN]` or `[STOP]
Supabase disk`, start here.**

```sql
-- the GRN schedulers genuinely need more room, and the volume has space:
UPDATE etl_disk_policy SET budget_gb = 8 WHERE pipeline = 'grn';

-- you resized the Supabase volume (do this EVERY time you resize):
UPDATE etl_disk_policy SET budget_gb = 100 WHERE pipeline = '_disk';

-- someone else should get the emails:
UPDATE etl_alert_config SET recipients = ARRAY['birbal@thebakersdozen.in'];
```

All thirteen GRN schedulers share one `grn` budget, because they are one
workload from the volume's point of view. A `[STOP]` means this scheduler is
refusing to write until you do one of the above. Nothing is lost: it stops
before writing and the next run continues.

`etl_alerts.py` is **identical in every pipeline repo** - do not add per-repo
logic to it. Everything configurable lives in Postgres (`etl_disk_policy`,
`etl_alert_config`), so budgets, thresholds and recipients change with an
`UPDATE` and no deploy, for all pipelines at once.

Three behaviours worth knowing:

- **No new credentials were needed.** This repo has no Postgres driver and no
  DSN, so the guard reaches the policy through PostgREST RPC using the
  `SUPABASE_URL` + service-role key it already holds.
- **It fails OPEN.** If the guard cannot run - credentials missing, database
  unreachable - it logs an error and lets the scheduler continue. A guard that
  breaks a working pipeline is worse than one that cannot check. Grep for
  `Disk guard could not run`.
- **Budgets grow themselves** into genuinely unallocated volume space, so a
  pipeline that is legitimately growing is not blocked by a number somebody
  guessed months ago. It can never grow past the volume ceiling.

Full documentation:
https://github.com/keyur-tbd/bc-supabase-sync#disk-alerts-and-auto-budgeting---start-here-if-you-got-an-email
