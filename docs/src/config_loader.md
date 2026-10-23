# `config_loader.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)

<a id="oppsummering"></a>
## Oppsummering

Validerer tabell- og kolonneoppsett fra YAML med Pydantic og gjør dem om til [`DimTable`](class_table.md#dimtable) eller [`FakTable`](class_table.md#faktable). [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/config_loader.py).

<a id="klasser"></a>
## Klasser

- [`YamlValueError`](#yamlvalueerror) markerer ugyldig YAML-verdi.
- [`ColDataTypes`](#coldatatypes), [`ColMode`](#colmode) og [`TableTypes`](#tabletypes) definerer tillatte verdier.
- [`ColumnModel`](#columnmodel) beskriver en kolonne.
- [`TableModel`](#tablemodel) beskriver en tabell.

<a id="yamlvalueerror"></a>
### `YamlValueError`

Unntak brukt av validatorene ved ugyldig tabell- eller kolonneoppsett.

**Arver fra:** `BaseException`.

**Konstruktør:** `YamlValueError.__init__(message: str)`.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`validate_col_data_type()`](#validate_col_data_type) | Oppretter unntak ved ugyldig verdi |
| [`validate_mode()`](#validate_mode) | Oppretter unntak ved ugyldig verdi |
| [`validate_table_type()`](#validate_table_type) | Oppretter unntak ved ugyldig verdi |
| [`validate_cols()`](#validate_cols) | Oppretter unntak ved ugyldig verdi |
| [`validate_check_col()`](#validate_check_col) | Oppretter unntak ved ugyldig verdi |
| [`validate_order_cols()`](#validate_order_cols) | Oppretter unntak ved ugyldig verdi |

**Metoder**

<a id="yaml-init"></a>
#### `__init__()`

Lagrer feilmeldingen i unntaket.

**Args:**

- `message` (`str`): Feilmeldingen for en ugyldig konfigurasjonsverdi.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`validate_col_data_type()`](#validate_col_data_type) | Kaller |
| [`validate_mode()`](#validate_mode) | Kaller |
| [`validate_table_type()`](#validate_table_type) | Kaller |
| [`validate_cols()`](#validate_cols) | Kaller |
| [`validate_check_col()`](#validate_check_col) | Kaller |
| [`validate_order_cols()`](#validate_order_cols) | Kaller |

<a id="coldatatypes"></a>
### `ColDataTypes`

Enum for `INTEGER`, `STRING`, `DATE` og `DATETIME`. Brukes av [`validate_col_data_type()`](#validate_col_data_type). Ingen egne metoder.

**Arver fra:** `Enum`.

Ingen eksplisitt annotert konstruktør i modulen.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`validate_col_data_type()`](#validate_col_data_type) | Sjekker tillatte datatyper |

<a id="colmode"></a>
### `ColMode`

Enum for `REQUIRED`. Brukes av [`validate_mode()`](#validate_mode). Ingen egne metoder.

**Arver fra:** `Enum`.

Ingen eksplisitt annotert konstruktør i modulen.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`validate_mode()`](#validate_mode) | Sjekker tillatt mode |

<a id="tabletypes"></a>
### `TableTypes`

Enum for `DIM` og `FAK`. Brukes ved tabellvalidering og konvertering. Ingen egne metoder.

**Arver fra:** `Enum`.

Ingen eksplisitt annotert konstruktør i modulen.

Ingen eksplisitt annoterte felt i klassen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`validate_table_type()`](#validate_table_type) | Sjekker tillatt tabelltype |
| [`to_table_object()`](#table_to_table_object) | Velger tabellobjekt |
| [`validate_check_col()`](#validate_check_col) | Sjekker faktatabell |
| [`validate_order_cols()`](#validate_order_cols) | Sjekker faktatabell |

<a id="columnmodel"></a>
### `ColumnModel`

Pydantic-modell for navn, datatype, beskrivelse, mode og valgfri maksimumslengde. Opprettes av [`from_dict()`](#column_from_dict) ved lesing av konfigurasjonen.

**Arver fra:** `BaseModel`.

**Opprettelse**

Pydantic tar imot disse feltene når `ColumnModel` opprettes:

**Args:**

- `name` (`str`): Kolonnenavnet. Påkrevd.
- `col_data_type` (`str`): Datatypen. Påkrevd og validert mot `ColDataTypes`.
- `description` (`str`): Kolonnebeskrivelsen. Påkrevd.
- `mode` (`Optional[str]`): Kolonnens mode. Standardverdi: `None`; angitt verdi valideres mot `ColMode`.
- `max_length` (`Optional[int]`): Maksimumslengden. Standardverdi: `None`; angitt verdi må være større enn null.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`ColumnModel.from_dict()`](#column_from_dict) | Oppretter instans |

**Metoder**

<a id="to_bq_schema_field"></a>
#### `to_bq_schema_field()`

Gjør kolonnen om til et BigQuery-`SchemaField`.

**Returns:**

`bigquery.SchemaField`: Kolonnen som BigQuery-skjemaelement.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`TableModel.to_table_object()`](#table_to_table_object) | Kaller |

<a id="column_from_dict"></a>
#### `from_dict()`

Leser kolonnefelter fra en dictionary og oppretter en validert `ColumnModel`.

**Args:**

- `col_dict` (`dict`): Kolonnedataene fra YAML-konfigurasjonen.

**Returns:**

`'ColumnModel'`: En validert kolonnemodell.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`TableModel.from_dict()`](#table_from_dict) | Kaller |

<a id="validate_col_data_type"></a>
#### `validate_col_data_type()`

Sjekker at kolonnedatatypen finnes i `ColDataTypes`, uavhengig av store/små bokstaver.

**Args:**

- `col_data_type` (`str`): Datatypen som skal valideres.

**Returns:**

`str`: Den validerte datatypen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Pydantic | Feltvalidator ved opprettelse av ColumnModel |

<a id="validate_mode"></a>
#### `validate_mode()`

Validerer angitt kolonne-mode mot `ColMode`; en manglende verdi er tillatt.

**Args:**

- `mode` (`Optional[str]`): Kolonnens mode, eller None hvis den mangler.

**Returns:**

`str`: Den validerte modusen eller None hvis feltet mangler.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Pydantic | Feltvalidator ved opprettelse av ColumnModel |

<a id="tablemodel"></a>
### `TableModel`

Pydantic-modell for tabelltype, beskrivelse, kolonner og eventuelle filter-/sorteringskolonner. Opprettes av [`load_config_tables()`](functions.md#load_config_tables) via [`from_dict()`](#table_from_dict).

**Arver fra:** `BaseModel`.

**Opprettelse**

Pydantic tar imot disse feltene når `TableModel` opprettes:

**Args:**

- `name` (`str`): Tabellnavnet. Påkrevd.
- `table_type` (`str`): Tabelltypen `DIM` eller `FAK`. Påkrevd.
- `description` (`str`): Tabellbeskrivelsen. Påkrevd.
- `cols` (`list[ColumnModel]`): Kolonnene. Påkrevd; må inneholde minst én kolonne.
- `check_col` (`Optional[str]`): Kontrollkolonnen. Standardverdi: `None`. Påkrevd for `FAK` og må finnes i `cols`; skal ikke settes for `DIM`.
- `order_cols` (`Optional[list[str]]`): Sorteringskolonnene. Standardverdi: `None`. Påkrevd for `FAK`, og alle må finnes i `cols`; skal ikke settes for `DIM`.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`TableModel.from_dict()`](#table_from_dict) | Oppretter instans |

**Metoder**

<a id="table_to_table_object"></a>
#### `to_table_object()`

Gjør kolonnene om til BigQuery-felter og oppretter `DimTable` eller `FakTable` ut fra tabelltypen.

**Returns:**

`Union[DimTable, FakTable]`: En dimensjons- eller faktatabell.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`load_config_tables()`](functions.md#load_config_tables) | Kaller |

<a id="table_from_dict"></a>
#### `from_dict()`

Leser tabellfelter og kolonner fra en dictionary og oppretter en validert `TableModel`.

**Args:**

- `table_dict` (`dict`): Tabelldataene fra YAML-konfigurasjonen.

**Returns:**

`'TableModel'`: En validert tabellmodell.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`load_config_tables()`](functions.md#load_config_tables) | Kaller |

<a id="validate_table_type"></a>
#### `validate_table_type()`

Sjekker at typen er `DIM` eller `FAK`.

**Args:**

- `table_type` (`str`): Tabelltypen som skal valideres.

**Returns:**

`str`: Den validerte tabelltypen.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Pydantic | Feltvalidator ved opprettelse av TableModel |

<a id="validate_cols"></a>
#### `validate_cols()`

Sikrer at minst én kolonne er konfigurert.

**Args:**

- `cols` (`list[ColumnModel]`): Kolonnemodellene som skal valideres.

**Returns:**

`list[ColumnModel]`: De validerte kolonnemodellene.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Pydantic | Feltvalidator ved opprettelse av TableModel |

<a id="validate_check_col"></a>
#### `validate_check_col()`

Krever at faktatabeller har en kontrollkolonne som finnes blant kolonnene, og at dimensjoner ikke har den.

**Returns:**

`Self`: Tabellmodellen etter validering.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Pydantic | Modellvalidator ved opprettelse av TableModel |

<a id="validate_order_cols"></a>
#### `validate_order_cols()`

Krever sorteringskolonner for fakta og sjekker at de finnes blant kolonnene; dimensjoner kan ikke ha sorteringskolonner.

**Returns:**

`Self`: Tabellmodellen etter validering.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Pydantic | Modellvalidator ved opprettelse av TableModel |

[Til oversikten](../index.md)
