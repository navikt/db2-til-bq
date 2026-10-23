# `functions.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)
- [Funksjoner](#funksjoner)

<a id="oppsummering"></a>
## Oppsummering

Hjelpefunksjoner for miljø, tabellkonfigurasjon og overføring fra DB2 til BigQuery. [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/functions.py). Kjøringene via `db2_til_bq_OS.py` og `db2_til_bq_UR.py` bruker `db2_to_bq()`; `init_OS.py` bruker noen av hjelpefunksjonene, men har sin egen overføringsfunksjon.

<a id="klasser"></a>
## Klasser

Ingen klasser er definert i denne modulen.

<a id="funksjoner"></a>
## Funksjoner

- [`set_and_check_envs`](#set_and_check_envs)
- [`load_config_tables`](#load_config_tables)
- [`get_from_datetime`](#get_from_datetime)
- [`generate_limits`](#generate_limits)
- [`delete_table`](#delete_table)
- [`create_datasets`](#create_datasets)
- [`db2_to_bq`](#db2_to_bq)
- [`update_desc`](#update_desc)

<a id="set_and_check_envs"></a>
### `set_and_check_envs()`

Oppretter [`EnvHandler`](env_handler.md#envhandler), laster miljøvariabler og sjekker at nødvendige verdier finnes.

**Args:**

- `source_name` (`str`): Kildenavnet som brukes til miljø- eller konfigurasjonsfil.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `main()` i [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Kaller |
| `main()` i [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Kaller |
| `main()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |
| [`update_desc()`](#update_desc) | Kaller |

<a id="load_config_tables"></a>
### `load_config_tables()`

Leser YAML-filen og bygger [`TableModel`](config_loader.md#tablemodel) og tabellobjekter i samme rekkefølge.

**Args:**

- `config_path` (`str`): Stien til YAML-filen med tabelloppsett.

**Returns:**

`list[Union[DimTable, FakTable]]`: Tabellobjektene fra YAML-konfigurasjonen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `main()` i [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Kaller |
| `main()` i [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Kaller |
| `main()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |
| [`update_desc()`](#update_desc) | Kaller |

<a id="get_from_datetime"></a>
### `get_from_datetime()`

Henter `MAX(check_col)` fra en eksisterende BigQuery-tabell; ellers velges dagens dato minus 730 dager. Formaterer tidspunktet som streng. En eksisterende, tom tabell gir ingen egen reserveverdi i koden.

**Args:**

- `bq_client` (`BQConnector`): Klienten som utfører BigQuery-operasjoner.
- `table` (`Union[DimTable, FakTable]`): Tabellobjektet som skal leses eller behandles.
- `table_exists_in_bq` (`bool`): Om tabellen allerede finnes i BigQuery.

**Returns:**

`str`: Starttidspunktet formatert som tekst.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](#db2_to_bq) | Kaller for faktatabeller |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller for faktatabeller |

<a id="generate_limits"></a>
### `generate_limits()`

Lager en liste med første dag i hver måned fra startmåneden til og med måneden etter dagens dato.

**Args:**

- `start_datetime` (`datetime`): Starttidspunktet for månedsgrensene.

**Returns:**

`list[date]`: Første dag i hver måned i intervallet.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |

<a id="delete_table"></a>
### `delete_table()`

Videresender tabellnavn og datasett til [`BQConnector.delete_table()`](bigquery_connector.md#delete_table).

**Args:**

- `table` (`Union[DimTable, FakTable]`): Tabellobjektet som skal leses eller behandles.
- `bq_client` (`BQConnector`): Klienten som utfører BigQuery-operasjoner.
- `logger` (`Logger`): Loggeren som brukes til statusmeldinger.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

<a id="create_datasets"></a>
### `create_datasets()`

Går gjennom datasettlisten og kaller [`BQConnector.create_dataset()`](bigquery_connector.md#create_dataset) for hvert datasett.

**Args:**

- `datasets` (`list[str]`): Navnene på BigQuery-datasettene som skal opprettes.
- `bq_connector` (`BQConnector`): Klienten som oppretter BigQuery-datasettene.
- `logger` (`Logger`): Loggeren som brukes til statusmeldinger.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

<a id="db2_to_bq"></a>
### `db2_to_bq()`

Velger inkrementelt uttrekk og `WRITE_APPEND` for fakta eller full lesing og `WRITE_TRUNCATE` for dimensjoner. Bygger spørring og lastkonfigurasjon fra tabellobjektet, leser DB2 i biter på inntil 1 000 000 rader og sender ikke-tomme biter til [`put_rows_alt()`](bigquery_connector.md#put_rows_alt). Logger antall rader. Ved flere dimensjonsbiter erstatter hver lasting den forrige.

**Args:**

- `table` (`Union[DimTable, FakTable]`): Tabellobjektet som skal leses eller behandles.
- `bq_client` (`BQConnector`): Klienten som utfører BigQuery-operasjoner.
- `db2_conn` (`DB2Connector`): Tilkoblingen som leser rader fra DB2.
- `logger` (`Logger`): Loggeren som brukes til statusmeldinger.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `main()` i [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Kaller |
| `main()` i [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Kaller |

<a id="update_desc"></a>
### `update_desc()`

Laster miljø og tabellkonfigurasjon, og ber BigQuery-klienten oppdatere tabell- og kolonnebeskrivelser. Bruker filnavnmønsteret `{source_name}_tables.yaml`; dette er ikke samme sti som de vanlige entrypointene bruker.

**Args:**

- `logger` (`Logger`): Loggeren som brukes til statusmeldinger.
- `source_name` (`str`): Kildenavnet som brukes til miljø- eller konfigurasjonsfil.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

[Til oversikten](../index.md)
