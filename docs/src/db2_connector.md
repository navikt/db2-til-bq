# `db2_connector.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)

<a id="oppsummering"></a>
## Oppsummering

Oppretter DB2-tilkobling fra miljøvariabler og henter rader med parameterbinding. [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/db2_connector.py).

<a id="klasser"></a>
## Klasser

- [`DB2Connector`](#db2connector) samler tilkobling, spørringer og bitvis lesing.

<a id="db2connector"></a>
### `DB2Connector`

Brukes av begge vanlige entrypoints og av `init_OS.py`, via fabrikkmetoden [`create_connector_from_envs()`](#create_connector_from_envs).

**Arver fra:** Ingen eksplisitt baseklasse.

**Konstruktør:** `DB2Connector.__init__(database_name: str, host: str, port: str, username: str, password: str)`.

**Annoterte felt:** `connection: ibm_db.IBM_DBConnection`.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`create_connector_from_envs()`](#create_connector_from_envs) | Oppretter instans |

**Metoder**

<a id="init"></a>
#### `__init__()`

Lagrer tilkoblingsparametere og oppretter DB2-tilkoblingen via [`_create_connection()`](#create_connection).

**Args:**

- `database_name` (`str`): Navnet på DB2-databasen.
- `host` (`str`): Adressen til DB2-serveren.
- `port` (`str`): Porten til DB2-serveren.
- `username` (`str`): Brukernavnet for DB2-tilkoblingen.
- `password` (`str`): Passordet for DB2-tilkoblingen.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`create_connector_from_envs()`](#create_connector_from_envs) | Oppretter instans |

<a id="exec_immediate"></a>
#### `exec_immediate()`

Utfører en spørring umiddelbart på den åpne DB2-tilkoblingen.

**Args:**

- `query` (ikke annotert): SQL-spørringen som skal kjøres i DB2.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`create_connector_from_envs()`](#create_connector_from_envs) | Kaller når databasen heter TDB2 |

<a id="close"></a>
#### `close()`

Lukker DB2-tilkoblingen.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `main()` i [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Kaller etter vellykket overføring |
| `main()` i [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Kaller etter vellykket overføring |
| `main()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller etter vellykket overføring |

<a id="get_chunks"></a>
#### `get_chunks()`

Legger `OFFSET` og `FETCH NEXT` til spørringen, henter rader i biter og avslutter når resultatet har færre rader enn bitstørrelsen. Returnerer også siste bit dersom den er tom.

**Args:**

- `chunk_size` (`int`): Høyeste antall rader i hver bit.
- `base_query` (`str`): SQL-spørringen som radene skal hentes med.
- `binds` (`dict`): Bind-parametere for DB2-spørringen.

**Returns:**

`Iterator[list[dict[str, Any]]]`: En iterator med DB2-rader fordelt i biter.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`functions.db2_to_bq()`](functions.md#db2_to_bq) | Kaller |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |

<a id="get_rows"></a>
#### `get_rows()`

Forbereder og kjører DB2-spørringen med bind-parametere og returnerer radene som dictionaries.

**Args:**

- `query` (`str`): SQL-spørringen som skal kjøres i DB2.
- `binds` (`Dict[int, Any]`): Bind-parametere for DB2-spørringen.

**Returns:**

`List[Dict[str, Any]]`: Radene fra DB2-spørringen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`get_chunks()`](#get_chunks) | Kaller |

<a id="create_connection"></a>
#### `_create_connection()`

Åpner DB2-tilkoblingen med en DSN fra [`_create_dsn()`](#create_dsn).

**Returns:**

`ibm_db.IBM_DBConnection`: DB2-tilkoblingen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`__init__()`](#init) | Kaller |

<a id="create_dsn"></a>
#### `_create_dsn()`

Setter sammen DB2-parametere til tilkoblingsstrengen.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`_create_connection()`](#create_connection) | Kaller |

<a id="create_connector_from_envs"></a>
#### `create_connector_from_envs()`

Leser DB2-parametere fra miljøvariabler og oppretter tilkoblingen. For `TDB2` aktiveres også spørringsakselerasjon.

**Returns:**

`'DB2Connector'`: En DB2Connector opprettet fra miljøvariablene.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| `main()` i [`db2_til_bq_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_OS.py) | Kaller |
| `main()` i [`db2_til_bq_UR.py`](https://github.com/navikt/db2-til-bq/blob/main/db2_til_bq_UR.py) | Kaller |
| `main()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller |

[Til oversikten](../index.md)
