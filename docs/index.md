# DB2 til BigQuery

## Innhold

- [Oversikt](#oversikt)
- [Entrypoint-filene](#kilder)
- [Felles kjøreflyt](#flyt)
- [Python-moduler](#moduler)

<a id="oversikt"></a>
## Oversikt

Repoet inneholder Nais-jobber som leser tabeller fra DB2 og laster dem til BigQuery. Tabeller, kolonner og lastetype defineres i YAML; Python-koden står for oppkobling, uttrekk og lasting.

De vanlige entrypointene er [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) og [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py). Begge kaller [`db2_to_bq()`](src/functions.md#db2_to_bq) for hver konfigurert tabell.

<a id="kilder"></a>
## Entrypoint-filene

### `db2_til_bq_OS.py`

Bruker miljøet `OS` og [`table_configs/tables_OS.yaml`](https://github.com/navikt/db2-til-bq/blob/main/table_configs/tables_OS.yaml). Lesing og lasting følger den felles flyten under.

### `db2_til_bq_UR.py`

Bruker miljøet `UR` og [`table_configs/tables_UR.yaml`](https://github.com/navikt/db2-til-bq/blob/main/table_configs/tables_UR.yaml). Dataene og DB2-tilkoblingen bestemmes av dette miljøet og konfigurasjonen.

Begge Nais-jobbene er satt opp til kl. 05:00 UTC mandag til fredag. `init_OS.py` er et eget initialiseringsskript og er ikke del av denne felles kjørebanen.

<a id="flyt"></a>
## Felles kjøreflyt

1. **Oppstart:** Entrypointet oppretter en logger og kaller `main()`. [`set_and_check_envs()`](src/functions.md#set_and_check_envs) laster lokale miljøvariabler ved lokal kjøring, kopierer DB2-lisensfilen på Nais og kontrollerer påkrevde variabler.
2. **Konfigurasjon:** [`load_config_tables()`](src/functions.md#load_config_tables) leser `table_configs/tables_OS.yaml` eller `table_configs/tables_UR.yaml` og lager tabellobjekter. Deretter opprettes én [`BQConnector`](src/bigquery_connector.md#bqconnector) for hele kjøringen.
3. **Per tabell:** Det opprettes en ny [`DB2Connector`](src/db2_connector.md#db2connector). [`db2_to_bq()`](src/functions.md#db2_to_bq) velger oppførsel ut fra tabelltypen:
   - **Faktatabell:** Sjekk om tabellen finnes i BigQuery. Bruk `MAX(check_col)` som nedre grense når den finnes; ellers brukes dagens dato minus 730 dager. Les DB2 med `WHERE check_col > ?` og `ORDER BY`. BigQuery-jobben bruker `WRITE_APPEND` og daglig partisjonering.
   - **Dimensjonstabell:** Les alle konfigurerte kolonner fra DB2 uten datofilter. BigQuery-jobben bruker `WRITE_TRUNCATE`.
4. **Overføring:** [`get_chunks()`](src/db2_connector.md#get_chunks) henter inntil 1 000 000 rader per bit med `OFFSET` og `FETCH NEXT`. Ikke-tomme biter serialiseres som JSON og lastes til BigQuery. Lesingen slutter når DB2 returnerer færre enn 1 000 000 rader.
5. **Neste tabell:** DB2-tilkoblingen lukkes etter vellykket overføring. Løkken fortsetter til alle tabeller i YAML er behandlet.

> **Viktig om dimensjoner:** `WRITE_TRUNCATE` brukes for hver ikke-tomme bit. Dersom en dimensjonstabell gir flere biter, erstatter hver lasting resultatet av den forrige. Ved feil finnes ingen egen retry, og tilkoblingen lukkes ikke garantert.

<a id="moduler"></a>
## Python-moduler i `src/`

- [`bigquery_connector.py`](src/bigquery_connector.md) – BigQuery-spørringer og lasting
- [`class_table.py`](src/class_table.md) – tabelltyper og SQL
- [`config_loader.py`](src/config_loader.md) – YAML-validering
- [`db2_connector.py`](src/db2_connector.md) – DB2-tilkobling og uttrekk
- [`env_handler.py`](src/env_handler.md) – miljø og lisens
- [`exceptions.py`](src/exceptions.md) – feilklasser
- [`functions.py`](src/functions.md) – overføringsflyt og hjelpefunksjoner
- [`logger.py`](src/logger.md) – loggoppsett og formatering
