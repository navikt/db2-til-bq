# db2-til-bq

Naisjobb som henter data fra stormaskin og legger på bigquery.

Repoet er ikke åpent grunnet underliggende system er en del av kritisk utbetalingsinfrastruktur, og vi refererer til databaseskjemaer og tabellnavn i denne koden.

## Miljø
Det er avhengighetene i ```requirements.txt``` som blir brukt i dockerimaget, og som dependabot sjekker. Lokalt brukes [uv](https://docs.astral.sh/uv/) som pakkemanager, og da må vi også kjøre 

```uv pip compile pyproject.toml -o requirements.txt ```

når vi oppdaterer pakker.