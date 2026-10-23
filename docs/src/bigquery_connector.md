# `bigquery_connector.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)
- [Funksjoner](#funksjoner)

<a id="oppsummering"></a>
## Oppsummering

Utfører BigQuery-spørringer, sjekker tabeller og laster DB2-rader. Håndterer også datasett, sletting og beskrivelser. [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/bigquery_connector.py).

<a id="klasser"></a>
## Klasser

- [`BQConnector`](#bqconnector) gir ett grensesnitt mot BigQuery-klienten.

<a id="bqconnector"></a>
### `BQConnector`

Opprettes av `db2_til_bq_OS.py` og `db2_til_bq_UR.py`, `init_OS.py` og [`update_desc()`](functions.md#update_desc).

**Arver fra:** Ingen eksplisitt baseklasse.

**Konstruktør:** `BQConnector.__init__()`.

**Annoterte felt:** `client: bigquery.Client`.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Oppretter klienten |
| [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Oppretter klienten |
| [`update_desc()`](functions.md#update_desc) | Oppretter klienten |
| [`main() i init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Oppretter klienten |

**Metoder**

<a id="init"></a>
#### `__init__()`

Oppretter en BigQuery-klient.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `main()` i [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Oppretter instans |
| `main()` i [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Oppretter instans |
| `main()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Oppretter instans |
| [`update_desc()`](functions.md#update_desc) | Oppretter instans |

<a id="execute_query"></a>
#### `_execute_query()`

Starter en BigQuery-spørring og returnerer jobben.

**Args:**

- `query` (`str`): SQL-spørringen som skal kjøres i BigQuery.

**Returns:**

`bigquery.QueryJob`: Jobben for den startede BigQuery-spørringen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`query()`](#query) | Kaller |
| [`get_rows()`](#get_rows) | Kaller |

<a id="query"></a>
#### `query()`

Venter på at spørringen fullføres. Ved `BadRequest` tolkes jobbens feil av [`BigQueryErrors`](exceptions.md#bigqueryerrors) og kastes videre.

**Args:**

- `query` (`str`): SQL-spørringen som skal kjøres i BigQuery.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |

<a id="get_rows"></a>
#### `get_rows()`

Utfører en BigQuery-spørring og returnerer resultatene som dictionaries.

**Args:**

- `query` (`str`): SQL-spørringen som skal kjøres i BigQuery.

**Returns:**

`List[Dict[str, Any]]`: Resultatradene som dictionaries.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`get_from_datetime()`](functions.md#get_from_datetime) | Kaller |

<a id="put_rows"></a>
#### `put_rows()`

Laster rader med BigQuery-metoden `load_table_from_json()` og videresender tolkede `BadRequest`-feil.

**Args:**

- `rows` (`list`): Radene som skal lastes til BigQuery.
- `table_id` (`str`): ID-en til BigQuery-tabellen.
- `job_config` (`bigquery.LoadJobConfig`): Konfigurasjonen for BigQuery-lastejobben.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

<a id="put_rows_alt"></a>
#### `put_rows_alt()`

Serialiserer rader som newline-delimited JSON og laster dem fra en bytes-buffer. Setter `source_format` på lastkonfigurasjonen og venter på resultatet; `BadRequest`-feil tolkes og kastes videre.

**Args:**

- `rows` (`list[dict[str, Any]]`): Radene som skal lastes til BigQuery.
- `table_id` (`str`): ID-en til BigQuery-tabellen.
- `job_config` (`bigquery.LoadJobConfig`): Konfigurasjonen for BigQuery-lastejobben.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`functions.db2_to_bq()`](functions.md#db2_to_bq) | Kaller |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |

<a id="create_client"></a>
#### `_create_client()`

Oppretter SDK-klienten med tilgjengelige GCP-innstillinger.

**Returns:**

`bigquery.Client`: En BigQuery-klient.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`__init__()`](#init) | Kaller |

<a id="format_results"></a>
#### `_format_results()`

Venter på spørreresultatet og gjør hver rad om til en dictionary.

**Args:**

- `query_job` (`bigquery.QueryJob`): Spørringsjobben med resultatene som skal leses.

**Returns:**

`List[Dict[str, Any]]`: Resultatradene som dictionaries.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`get_rows()`](#get_rows) | Kaller |

<a id="create_dataset"></a>
#### `create_dataset()`

Oppretter et BigQuery-datasett i angitt region, som standard `europe-north1`.

**Args:**

- `dataset_name` (`str`): Navnet på BigQuery-datasettet.
- `logger` (`Logger`): Loggeren som brukes til statusmeldinger.
- `description` (`str`): Beskrivelsen av datasettet. Standardverdi: `None`.
- `location` (`str`): Regionen datasettet skal opprettes i. Standardverdi: `'europe-north1'`.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`create_datasets()`](functions.md#create_datasets) | Kaller |

<a id="delete_table"></a>
#### `delete_table()`

Sletter den angitte BigQuery-tabellen etter at prosjekt, datasett og navn er satt sammen.

**Args:**

- `table_name` (`str`): Navnet på tabellen som skal slettes.
- `dataset` (`str`): Datasettet tabellen tilhører.
- `logger` (`Logger`): Loggeren som brukes til statusmeldinger.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`functions.delete_table()`](functions.md#delete_table) | Kaller |

<a id="check_if_table_exists_in_bq"></a>
#### `check_if_table_exists_in_bq()`

Bruker `get_table()` og returnerer `False` ved `NotFound`, ellers `True`.

**Args:**

- `table_id` (`str`): ID-en til BigQuery-tabellen.

**Returns:**

`bool`: Om tabellen finnes i BigQuery.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`functions.db2_to_bq()`](functions.md#db2_to_bq) | Kaller for faktatabeller |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller for faktatabeller |

<a id="update_table_and_col_descriptions"></a>
#### `update_table_and_col_descriptions()`

Henter tabellen og oppdaterer beskrivelse og skjema dersom de skiller seg fra konfigurasjonen.

**Args:**

- `table_id` (`str`): ID-en til BigQuery-tabellen.
- `desc` (`str`): Den nye beskrivelsen av tabellen.
- `schema` (`List`): Kolonnene som skal brukes i BigQuery-skjemaet.
- `logger` (`Logger`): Loggeren som brukes til statusmeldinger.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`update_desc()`](functions.md#update_desc) | Kaller |

<a id="funksjoner"></a>
## Funksjoner

- [`json_serial`](#json_serial)

<a id="json_serial"></a>
### `json_serial()`

Gjør `date` og `datetime` om til ISO-strenger. Andre typer gir `TypeError`.

**Args:**

- `obj` (ikke annotert): Datoen eller tidspunktet som skal serialiseres til JSON.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`put_rows_alt()`](#put_rows_alt) | Registrerer callback hos json.dumps() |

[Til oversikten](../index.md)
