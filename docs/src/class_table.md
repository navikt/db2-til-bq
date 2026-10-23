# `class_table.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)

<a id="oppsummering"></a>
## Oppsummering

Beskriver tabelltypene og bygger DB2-spørringer, bind-parametere og BigQuery-lastkonfigurasjon. [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/class_table.py).

<a id="klasser"></a>
## Klasser

- [`TableType`](#tabletype) skiller `DIM` og `FAK`.
- [`BaseTable`](#basetable) er abstrakt fellesklasse.
- [`DimTable`](#dimtable) er dimensjon med full last.
- [`FakTable`](#faktable) er faktatabell med inkrementell last.

<a id="tabletype"></a>
### `TableType`

Enum brukt ved opprettelse av [`DimTable`](#dimtable) og [`FakTable`](#faktable), ved valg av uttrekk i [`db2_to_bq()`](functions.md#db2_to_bq) og ved filtrering i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py). Ingen egne metoder.

**Arver fra:** `Enum`.

Ingen eksplisitt annotert konstruktør i modulen.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`DimTable.__init__()`](#dim-init) | Angir tabelltype |
| [`FakTable.__init__()`](#fak-init) | Angir tabelltype |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Sjekker tabelltype |
| [`main()`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Filtrerer på tabelltype |

<a id="basetable"></a>
### `BaseTable`

Holder tabellnavn, beskrivelse, kolonner, DB2-skjema og BigQuery-mål. Bygger felles deler av spørring og lastkonfigurasjon. Arves av [`DimTable`](#dimtable) og [`FakTable`](#faktable); instansieres ikke direkte.

**Arver fra:** `ABC`.

**Konstruktør:** `BaseTable.__init__(name: str, description: str, cols: list[SchemaField], table_type: TableType) -> None`.

**Annoterte felt:** `_db2_schema: str`, `_bq_dataset: str`.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`DimTable`](#dimtable) | Arver fra BaseTable |
| [`FakTable`](#faktable) | Arver fra BaseTable |

**Metoder og egenskaper**

<a id="base-init"></a>
#### `__init__()`

Lagrer tabellmetadata og setter DB2-skjema og BigQuery-mål fra miljøet.

**Args:**

- `name` (`str`): Navnet på DB2-tabellen.
- `description` (`str`): Beskrivelsen av tabellen.
- `cols` (`list[SchemaField]`): Kolonnene i BigQuery-skjemaet.
- `table_type` (`TableType`): Tabelltypen DIM eller FAK.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`DimTable.__init__()`](#dim-init) | Kaller |
| [`FakTable.__init__()`](#fak-init) | Kaller |

<a id="set_envs"></a>
#### `_set_envs()`

Leser `DATABASE_SCHEMA`, bruker de to første tegnene som BigQuery-datasett og setter tabell-ID.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`BaseTable.__init__()`](#base-init) | Kaller |

<a id="name"></a>
#### `name` (egenskap)

Gir tabellnavnet.

**Returns:**

`str`: Tabellnavnet.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`_build_sql_db2()`](#build_sql_db2) | Leser egenskap |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Leser egenskap |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="description"></a>
#### `description` (egenskap)

Gir tabellbeskrivelsen.

**Returns:**

`str`: Tabellbeskrivelsen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`update_desc()`](functions.md#update_desc) | Leser egenskap |

<a id="cols"></a>
#### `cols` (egenskap)

Gir kolonnedefinisjonene.

**Returns:**

`list[SchemaField]`: Kolonnedefinisjonene.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`_build_sql_db2()`](#build_sql_db2) | Leser egenskap |
| [`_make_bq_load_job_config()`](#make_bq_load_job_config) | Leser egenskap |
| [`update_desc()`](functions.md#update_desc) | Leser egenskap |
| [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="db2_schema"></a>
#### `db2_schema` (egenskap)

Gir DB2-skjemaet.

**Returns:**

`str`: DB2-skjemaet.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`_build_sql_db2()`](#build_sql_db2) | Leser egenskap |
| [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="bq_dataset"></a>
#### `bq_dataset` (egenskap)

Gir BigQuery-datasettet.

**Returns:**

`str`: BigQuery-datasettet.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`delete_table()`](functions.md#delete_table) | Leser egenskap |

<a id="bq_table_id"></a>
#### `bq_table_id` (egenskap)

Gir BigQuery-tabellens datasett og navn.

**Returns:**

`str`: BigQuery-tabellens ID.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Leser egenskap |
| [`get_from_datetime()`](functions.md#get_from_datetime) | Leser egenskap |
| [`update_desc()`](functions.md#update_desc) | Leser egenskap |
| [`delete_table()`](functions.md#delete_table) | Leser egenskap |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="table_type"></a>
#### `table_type` (egenskap)

Gir `TableType` for tabellen.

**Returns:**

`TableType`: Tabellens type.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Leser egenskap |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="base-build_sql_db2"></a>
#### `build_sql_db2()` (abstrakt)

Definerer grensesnittet som underklassene må implementere; basemetoden kaster `NotImplementedError`.

**Returns:**

`str`: SQL-spørringen som underklassen skal bygge.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen direkte interne kallere | Abstrakt metode; underklassen kalles av db2_to_bq() |

<a id="build_sql_db2"></a>
#### `_build_sql_db2()`

Bygger en `SELECT`-spørring for konfigurerte kolonner fra DB2-tabellen.

**Returns:**

`str`: SQL-spørringen for de konfigurerte kolonnene.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`DimTable.build_sql_db2()`](#dim-build_sql_db2) | Kaller |
| [`FakTable.build_sql_db2()`](#fak-build_sql_db2) | Kaller |

<a id="base-make_bq_load_job_config"></a>
#### `make_bq_load_job_config()` (abstrakt)

Definerer grensesnittet for BigQuery-lastkonfigurasjon; basemetoden kaster `NotImplementedError`.

**Returns:**

`LoadJobConfig`: Lastkonfigurasjonen som underklassen skal bygge.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen direkte interne kallere | Abstrakt metode; underklassen kalles av db2_to_bq() |

<a id="make_bq_load_job_config"></a>
#### `_make_bq_load_job_config()`

Oppretter `LoadJobConfig` med kolonnene, `CREATE_IF_NEEDED` og angitt skrivemodus.

**Args:**

- `write_disposition` (`str`): Skrivemodus for BigQuery-lastejobben.

**Returns:**

`LoadJobConfig`: Lastkonfigurasjonen med angitt skrivemodus.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`DimTable.make_bq_load_job_config()`](#dim-make_bq_load_job_config) | Kaller |
| [`FakTable.make_bq_load_job_config()`](#fak-make_bq_load_job_config) | Kaller |

<a id="base-generate_binds"></a>
#### `generate_binds()` (abstrakt)

Definerer grensesnittet for bind-parametere; basemetoden kaster `NotImplementedError`.

**Returns:**

`dict[int, Any]`: Bind-parametere som underklassen skal bygge.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen direkte interne kallere | Abstrakt metode; underklassen kalles av db2_to_bq() |

<a id="generate_binds"></a>
#### `_generate_binds()`

Returnerer tomme bind-parametere for dimensjonstabeller.

**Returns:**

`dict[int, Any]`: Tomme bind-parametere.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`DimTable.generate_binds()`](#dim-generate_binds) | Kaller |

<a id="dimtable"></a>
### `DimTable`

Velger alle angitte DB2-kolonner og `WRITE_TRUNCATE` ved hver lasting. Opprettes av [`TableModel.to_table_object()`](config_loader.md#table_to_table_object).

**Arver fra:** `BaseTable`.

**Konstruktør:** `DimTable.__init__(name: str, description: str, cols: list[SchemaField]) -> None`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`TableModel.to_table_object()`](config_loader.md#table_to_table_object) | Oppretter instans ved dimensjonstabell |

**Metoder og egenskaper**

<a id="dim-init"></a>
#### `__init__()`

Oppretter et tabellobjekt med type `DIM`.

**Args:**

- `name` (`str`): Navnet på DB2-tabellen.
- `description` (`str`): Beskrivelsen av tabellen.
- `cols` (`list[SchemaField]`): Kolonnene i BigQuery-skjemaet.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`TableModel.to_table_object()`](config_loader.md#table_to_table_object) | Kaller for dimensjoner |

<a id="dim-build_sql_db2"></a>
#### `build_sql_db2()`

Returnerer en `SELECT`-spørring uten datofilter.

**Returns:**

`str`: SQL-spørringen uten datofilter.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Kaller for dimensjoner |

<a id="dim-make_bq_load_job_config"></a>
#### `make_bq_load_job_config()`

Lager en BigQuery-jobb med `WRITE_TRUNCATE`.

**Returns:**

`LoadJobConfig`: Lastkonfigurasjonen for full last.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Kaller for dimensjoner |

<a id="dim-generate_binds"></a>
#### `generate_binds()`

Returnerer en tom dictionary siden dimensjoner ikke har datofilter.

**Returns:**

`dict[int, Any]`: Tomme bind-parametere.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Kaller for dimensjoner |

<a id="faktable"></a>
### `FakTable`

Filtrerer på `check_col`, sorterer etter konfigurerte kolonner og bruker `WRITE_APPEND` med daglig tids-partisjonering. Opprettes av [`TableModel.to_table_object()`](config_loader.md#table_to_table_object).

**Arver fra:** `BaseTable`.

**Konstruktør:** `FakTable.__init__(name: str, description: str, cols: list[SchemaField], check_col: str, order_cols: list[str]) -> None`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`TableModel.to_table_object()`](config_loader.md#table_to_table_object) | Oppretter instans ved faktatabell |

**Metoder og egenskaper**

<a id="fak-init"></a>
#### `__init__()`

Oppretter et faktatabellobjekt og lagrer filter- og sorteringskolonnene.

**Args:**

- `name` (`str`): Navnet på DB2-tabellen.
- `description` (`str`): Beskrivelsen av tabellen.
- `cols` (`list[SchemaField]`): Kolonnene i BigQuery-skjemaet.
- `check_col` (`str`): Kolonnen som brukes til inkrementelt uttrekk.
- `order_cols` (`list[str]`): Kolonnene DB2-resultatet sorteres etter.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`TableModel.to_table_object()`](config_loader.md#table_to_table_object) | Kaller for faktatabeller |

<a id="check_col"></a>
#### `check_col` (egenskap)

Gir navnet på kolonnen som brukes til inkrementelt uttrekk og partisjonering.

**Returns:**

`str`: Kontrollkolonnen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`get_from_datetime()`](functions.md#get_from_datetime) | Leser egenskap |
| [`FakTable.build_sql_db2()`](#fak-build_sql_db2) | Leser egenskap |
| [`FakTable.make_bq_load_job_config()`](#fak-make_bq_load_job_config) | Leser egenskap |
| [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="order_cols"></a>
#### `order_cols` (egenskap)

Gir kolonnene som DB2-resultatet sorteres etter.

**Returns:**

`list[str]`: Sorteringskolonnene.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`FakTable.build_sql_db2()`](#fak-build_sql_db2) | Leser egenskap |
| [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="from_datetime"></a>
#### `from_datetime` (egenskap)

Gir nedre grense for faktaspørringen.

**Returns:**

`str`: Starttidspunktet for inkrementelt uttrekk.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`FakTable.generate_binds()`](#fak-generate_binds) | Leser egenskap |
| [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Leser egenskap |

<a id="from_datetime_setter"></a>
#### `from_datetime()` (setter)

Lagrer nedre grense for faktaspørringen.

**Args:**

- `from_datetime` (`str`): Starttidspunktet for inkrementelt uttrekk.

**Returns:**

`None`: Ingen returverdi.

Setter for en `@property`.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`functions.db2_to_bq()`](functions.md#db2_to_bq) | Setter egenskap |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Setter egenskap |

<a id="fak-build_sql_db2"></a>
#### `build_sql_db2()`

Utvider `SELECT` med `WHERE check_col > ?` og `ORDER BY`.

**Returns:**

`str`: SQL-spørringen med dato- og sorteringsfilter.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Kaller for faktatabeller |

<a id="fak-make_bq_load_job_config"></a>
#### `make_bq_load_job_config()`

Lager `WRITE_APPEND`-jobb med daglig tids-partisjonering og 730 dagers utløp.

**Returns:**

`LoadJobConfig`: Lastkonfigurasjonen for inkrementell last.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Kaller for faktatabeller |
| `db2_to_bq()` i [`init_OS.py`](https://github.com/navikt/db2-til-bq/blob/main/init_OS.py) | Kaller for faktatabeller |

<a id="fak-generate_binds"></a>
#### `generate_binds()`

Returnerer `from_datetime` som bind-parameter nummer 1.

**Returns:**

`dict[int, Any]`: Starttidspunktet som bind-parameter.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`db2_to_bq()`](functions.md#db2_to_bq) | Kaller for faktatabeller |

[Til oversikten](../index.md)
