# `exceptions.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)

<a id="oppsummering"></a>
## Oppsummering

Egendefinerte unntak for miljø, lisens og BigQuery, og en klasse som tolker feildetaljer fra BigQuery. [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/exceptions.py).

<a id="klasser"></a>
## Klasser

- [`EnvsNotSetError`](#envsnotseterror)
- [`Db2LicenseNotFoundError`](#db2licensenotfounderror)
- [`BigQueryError`](#bigqueryerror)
- [`BigQueryInvalidValueError`](#bigqueryinvalidvalueerror)
- [`BigQueryMissingRequiredError`](#bigquerymissingrequirederror)
- [`BigQueryErrors`](#bigqueryerrors)

<a id="envsnotseterror"></a>
### `EnvsNotSetError`

Kastes av [`EnvHandler.check_envs()`](env_handler.md#check_envs) når miljøvariabler mangler.

**Arver fra:** `BaseException`.

**Konstruktør:** `EnvsNotSetError.__init__(message: str) -> None`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`EnvHandler.check_envs()`](env_handler.md#check_envs) | Oppretter unntak ved manglende variabler |

**Metoder**

<a id="envs-init"></a>
#### `__init__()`

Lagrer meldingen om manglende variabler.

**Args:**

- `message` (`str`): Feilmeldingen som lagres i unntaket.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`check_envs()`](env_handler.md#check_envs) | Kaller |

<a id="db2licensenotfounderror"></a>
### `Db2LicenseNotFoundError`

Kastes av [`EnvHandler._copy_db2_license()`](env_handler.md#copy_db2_license) hvis lisensfilen mangler på Nais.

**Arver fra:** `BaseException`.

**Konstruktør:** `Db2LicenseNotFoundError.__init__(message: str) -> None`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`EnvHandler._copy_db2_license()`](env_handler.md#copy_db2_license) | Oppretter unntak når lisensfil mangler |

**Metoder**

<a id="license-init"></a>
#### `__init__()`

Lagrer meldingen om manglende lisensfil.

**Args:**

- `message` (`str`): Feilmeldingen som lagres i unntaket.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`_copy_db2_license()`](env_handler.md#copy_db2_license) | Kaller |

<a id="bigqueryerror"></a>
### `BigQueryError`

Generelt unntak for BigQuery-feil som ikke passer de to mer spesifikke kategoriene. Opprettes av [`BigQueryErrors._process_errors()`](#process_errors).

**Arver fra:** `BaseException`.

**Konstruktør:** `BigQueryError.__init__(message: str) -> None`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BigQueryErrors._process_errors()`](#process_errors) | Oppretter generelt unntak |

**Metoder**

<a id="bqerror-init"></a>
#### `__init__()`

Lagrer en generell BigQuery-feilmelding.

**Args:**

- `message` (`str`): Feilmeldingen som lagres i unntaket.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BigQueryErrors._process_errors()`](#process_errors) | Kaller |

<a id="bigqueryinvalidvalueerror"></a>
### `BigQueryInvalidValueError`

Brukes når feilmeldingen tolkes som ugyldig kolonneverdi. Opprettes av [`_process_errors()`](#process_errors).

**Arver fra:** `BaseException`.

**Konstruktør:** `BigQueryInvalidValueError.__init__(message: str) -> None`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BigQueryErrors._process_errors()`](#process_errors) | Oppretter unntak ved ugyldig verdi |

**Metoder**

<a id="invalid-init"></a>
#### `__init__()`

Lagrer feilmeldingen for en ugyldig verdi.

**Args:**

- `message` (`str`): Feilmeldingen som lagres i unntaket.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BigQueryErrors._process_errors()`](#process_errors) | Kaller |

<a id="bigquerymissingrequirederror"></a>
### `BigQueryMissingRequiredError`

Brukes når feilmeldingen tolkes som manglende verdi. Opprettes av [`_process_errors()`](#process_errors).

**Arver fra:** `BaseException`.

**Konstruktør:** `BigQueryMissingRequiredError.__init__(message: str) -> None`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BigQueryErrors._process_errors()`](#process_errors) | Oppretter unntak ved manglende verdi |

**Metoder**

<a id="missing-init"></a>
#### `__init__()`

Lagrer feilmeldingen for en manglende verdi.

**Args:**

- `message` (`str`): Feilmeldingen som lagres i unntaket.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BigQueryErrors._process_errors()`](#process_errors) | Kaller |

<a id="bigqueryerrors"></a>
### `BigQueryErrors`

Gjør listen over BigQuery-jobbfeil om til iterable unntak. Opprettes av [`BQConnector.query()`](bigquery_connector.md#query), [`put_rows()`](bigquery_connector.md#put_rows) og [`put_rows_alt()`](bigquery_connector.md#put_rows_alt) ved `BadRequest`.

**Arver fra:** Ingen eksplisitt baseklasse.

**Konstruktør:** `BigQueryErrors.__init__(errors: Union[list[dict[str, str]], Any]) -> None`.

**Annoterte felt:** `_exceptions: list[Union[BigQueryInvalidValueError, BigQueryMissingRequiredError, BigQueryError]]`.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BQConnector.query()`](bigquery_connector.md#query) | Oppretter feiltolker ved BadRequest |
| [`BQConnector.put_rows()`](bigquery_connector.md#put_rows) | Oppretter feiltolker ved BadRequest |
| [`BQConnector.put_rows_alt()`](bigquery_connector.md#put_rows_alt) | Oppretter feiltolker ved BadRequest |

**Metoder**

<a id="errors-init"></a>
#### `__init__()`

Lagrer feillisten og kaller [`_process_errors()`](#process_errors) for å lage unntakene.

**Args:**

- `errors` (`Union[list[dict[str, str]], Any]`): Feildetaljene fra BigQuery-jobben.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BQConnector.query()`](bigquery_connector.md#query) | Kaller |
| [`put_rows()`](bigquery_connector.md#put_rows) | Kaller |
| [`put_rows_alt()`](bigquery_connector.md#put_rows_alt) | Kaller |

<a id="process_errors"></a>
#### `_process_errors()`

Går gjennom feilene, ser på ordene i meldingene og velger en av de tre BigQuery-unntakstypene.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BigQueryErrors.__init__()`](#errors-init) | Kaller |

<a id="iter"></a>
#### `__iter__()`

Returnerer unntakene ett etter ett til den som itererer over objektet.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BQConnector.query()`](bigquery_connector.md#query) | Itererer over unntak |
| [`put_rows()`](bigquery_connector.md#put_rows) | Itererer over unntak |
| [`put_rows_alt()`](bigquery_connector.md#put_rows_alt) | Itererer over unntak |

[Til oversikten](../index.md)
