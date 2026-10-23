# `logger.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)

<a id="oppsummering"></a>
## Oppsummering

Setter opp Python-logging med farger i konsollen og valgfri fil. [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/logger.py).

<a id="klasser"></a>
## Klasser

- [`Logger`](#logger) gir metoder for loggnivåene.
- [`StreamFormatter`](#streamformatter) farger konsollogg etter nivå.

<a id="logger"></a>
### `Logger`

Opprettes ved oppstart av [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py), [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) og [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py).

**Arver fra:** Ingen eksplisitt baseklasse.

**Konstruktør:** `Logger.__init__(name: str, log_to_file: bool = False, level: int = logging.DEBUG)`.

**Annoterte felt:** `_logger: logging.Logger`.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Oppretter logger ved oppstart |
| [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Oppretter logger ved oppstart |
| [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Oppretter logger ved oppstart |

**Metoder**

<a id="init"></a>
#### `__init__()`

Lagrer innstillinger og kaller [`_configure_logger()`](#configure_logger). Standardnivå er `DEBUG`; fillogging er av som standard.

**Args:**

- `name` (`str`): Navnet som identifiserer loggeren.
- `log_to_file` (`bool`): Om loggen også skal skrives til fil. Standardverdi: `False`.
- `level` (`int`): Loggnivået som skal brukes. Standardverdi: `logging.DEBUG`.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Oppretter instans |
| [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Oppretter instans |
| [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Oppretter instans |

<a id="log"></a>
#### `_log()`

Sender meldingen til Pythons logger med angitt nivå.

**Args:**

- `message` (`str`): Meldingen som skal logges.
- `level` (`int`): Loggnivået som skal brukes.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`debug()`](#debug) | Kaller |
| [`info()`](#info) | Kaller |
| [`warning()`](#warning) | Kaller |
| [`error()`](#error) | Kaller |
| [`critical()`](#critical) | Kaller |

<a id="debug"></a>
#### `debug()`

Logger på `DEBUG`-nivå.

**Args:**

- `message` (`str`): Meldingen som skal logges.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

<a id="info"></a>
#### `info()`

Logger på `INFO`-nivå.

**Args:**

- `message` (`str`): Meldingen som skal logges.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Kaller |
| [`update_desc()`](functions.md#update_desc) | Kaller |
| [`BQConnector.create_dataset()`](bigquery_connector.md#create_dataset) | Kaller |
| [`delete_table()`](bigquery_connector.md#delete_table) | Kaller |
| [`update_table_and_col_descriptions()`](bigquery_connector.md#update_table_and_col_descriptions) | Kaller |
| `main()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |

<a id="warning"></a>
#### `warning()`

Logger på `WARNING`-nivå.

**Args:**

- `message` (`str`): Meldingen som skal logges.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

<a id="error"></a>
#### `error()`

Logger på `ERROR`-nivå.

**Args:**

- `message` (`str`): Meldingen som skal logges.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

<a id="critical"></a>
#### `critical()`

Logger på `CRITICAL`-nivå.

**Args:**

- `message` (`str`): Meldingen som skal logges.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Ikke brukt i repoet |

<a id="configure_logger"></a>
#### `_configure_logger()`

Konfigurerer konsollhandleren med `StreamFormatter`, eventuelt også en filhandler, og setter loggnivå og propagasjon.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`Logger.__init__()`](#init) | Kaller |

<a id="streamformatter"></a>
### `StreamFormatter`

Underklasse av `logging.Formatter` som velger format etter loggnivå. Opprettes av [`Logger._configure_logger()`](#configure_logger).

**Arver fra:** `logging.Formatter`.

Ingen eksplisitt annotert konstruktør i modulen.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`Logger._configure_logger()`](#configure_logger) | Oppretter formaterer |

**Metoder**

<a id="format"></a>
#### `format()`

Velger farget formatstreng ut fra nivået og formaterer loggposten.

**Args:**

- `record` (ikke annotert): Loggposten som skal formateres.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `logging.StreamHandler` | Callback ved formatering av loggposter |

[Til oversikten](../index.md)
