# db2-til-bq

Naisjobb som henter data fra stormaskin og legger på bigquery.

## Komme i gang

### Miljø
Vi bruker uv for lokal kjøring. Kjør `uv sync` ved første gang man skal kjøre kode lokalt, eller `uv sync --upgrade` for å oppdatere pakker.

For å oppdatere pakker så oppdateres `pyproject.toml` og kjører 

`toml-to-req --toml-file pyproject.toml`

Her bruker vi pakka [toml-to-requirements](https://pypi.org/project/toml-to-requirements/).

Det er avhengighetene i `requirements.txt` som blir brukt i dockerimaget, og som dependabot sjekker. Lokalt brukes [uv](https://docs.astral.sh/uv/) som pakkemanager.

(Var vanskelig å bruke `pyproject.toml` for docker og dependabot, men vi ønsker likevel å bruke uv.)

### GCP login
Lokalt brukes OAuth:

`gcloud auth application-default login`

I nais-jobben har vi en egen servicebruker, `nais-job-srv`. Den har en egen nøkkel som ligger som nais secret, og blir hentet ut når nais-jobben kjører.

#### Oppdatere tabeller i bigquery
I filen `src/config_tables.py` ligger listen med tabeller og medfølgende beskrivelser og schema for kolonnene. Når man oppdaterer disse må man også manuelt kjøre `update_desc()` i `main.py`. Dersom man endrer `mode` eller `max_length` på en kolonne, må man huske å slette tabellen først.

### db2 login

Trenger en `.env` fil lokalt (filnavn lagt i `.gitignore`), som inneholder følgende (på følgende format, uten noen fnutter):

DATABASE_USERNAME=xxx
DATABASE_PASSWORD=xxx
DATABASE_SCHEMA=xxx
DATABASE_NAME=xxx
DATABASE_PORT=xxx
DATABASE_HOST=xxx
GOOGLE_CLOUD_PROJECT=xxx

(På nais så blir disse variablene lastet inn i podden via nais Secrets.)

Må også legge til IBM db2 lisensfil for Python her `.venv/lib/python3.13/site-packages/clidriver/license`. Denne finnes lagret som nais secret [her](https://console.nav.cloud.nais.io/team/utsikt/dev-fss/secret/db2-license-12)

Obs! nais-jobben bruker versjon 11, mens lokalt (med nye Mac'er) brukes versjon 12. Begge er lagret som nais secrets.

### Formattering
Vi bruker [Ruff](https://docs.astral.sh/ruff/tutorial/) til formattering av kode.

### Alerting og logger
Varsel er satt opp i [alert.yaml](https://github.com/navikt/db2-til-bq/blob/main/.nais/alert.yaml) ved hjelp av Prometheus, og deployet med [deploy-alerts.yaml](https://github.com/navikt/db2-til-bq/blob/main/.github/workflows/deploy-alerts.yaml).

Loggene kan bli sett i [Grafana Loki](https://grafana.nav.cloud.nais.io/a/grafana-lokiexplore-app/). Mer om [observability i nais](https://docs.nais.io/observability/).

For å slette jobben må man da inn i kubectl og finner jobber ved `kubectl get jobs`  og så sletter det med `kubectl delete job <jobname>` . Vi har gjort en feature request til nais om å få dette inn i naisconsollen, fordi i prod krever det en del innlogging for å kunne slette jobber fra consollen.

---

## Henvendelser

Spørsmål knyttet til koden eller repositoryet kan stilles som issues her på GitHub

### For Nav-ansatte

Interne henvendelser kan sendes via Slack i kanalen #team-utsikt.