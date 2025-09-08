# db2-til-bq

Naisjobb som henter data fra stormaskin og legger på bigquery.

Repoet er ikke åpent grunnet underliggende system er en del av kritisk utbetalingsinfrastruktur, og vi refererer til databaseskjemaer og tabellnavn i denne koden.

## Miljø
Vi bruker uv for lokal kjøring. Kjør `uv sync` ved første gang man skal kjøre kode lokalt.

Det er avhengighetene i `requirements.txt` som blir brukt i dockerimaget, og som dependabot sjekker. Lokalt brukes [uv](https://docs.astral.sh/uv/) som pakkemanager, og da må vi også kjøre 

`uv pip compile pyproject.toml -o requirements.txt`

når vi oppdaterer pakker.

(Var vanskelig å bruke `pyproject.toml` for docker og dependabot, men vi ønsker likevel å bruke uv.)

## GCP login
Lokalt brukes OAuth:

`gcloud auth application-default login`

I nais-jobben har vi en egen servicebruker, `nais-job-srv`. Den har en egen nøkkel som ligger som nais secret, og blir hentet ut når nais-jobben kjører.

## db2 login

Trenger en `.env` fil lokalt (filnavn lagt i `.gitignore`), som inneholder følgende (på følgende format, uten noen fnutter):

DATABASE_USERNAME=xxx
DATABASE_PASSWORD=xxx
DATABASE_SCHEMA=xxx
DATABASE_NAME=xxx
DATABASE_PORT=xxx
DATABASE_HOST=xxx

(På nais så blir disse variablene lastet inn i podden via nais Secrets.)

Må også legge til IBM db2 lisensfil for Python her `.venv/lib/python3.13/site-packages/clidriver/license`. Denne finnes lagret som nais secret [her](https://console.nav.cloud.nais.io/team/utsikt/dev-fss/secret/db2-license-12)

Obs! nais-jobben bruker versjon 11, mens lokalt (med nye Mac'er) brukes versjon 12. Begge er lagret som nais secrets.

## Formattering
Bruker Black formatter til formattering av kode.