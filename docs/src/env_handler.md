# `env_handler.py`

## Innhold

- [Oppsummering](#oppsummering)
- [Klasser](#klasser)

<a id="oppsummering"></a>
## Oppsummering

Laster lokale miljøvariabler, kontrollerer påkrevde verdier og kopierer DB2-lisensfilen i Nais-miljøet. [Se kildefilen](https://github.com/navikt/db2-til-bq/blob/main/src/env_handler.py).

<a id="klasser"></a>
## Klasser

- [`EnvHandler`](#envhandler) er en dataklasse for kildeavhengig miljøoppsett.

<a id="envhandler"></a>
### `EnvHandler`

Opprettes av [`set_and_check_envs()`](functions.md#set_and_check_envs) med `OS` eller `UR` som kildenavn.

**Arver fra:** Ingen eksplisitt baseklasse.

**Opprettelse**

`@dataclass` genererer `EnvHandler(source_name: str)`.

**Args:**

- `source_name` (`str`): Kildenavnet som brukes til miljøoppsettet.

**Felt satt i [`__post_init__()`](#post_init):**

Disse feltene tas ikke imot av konstruktøren.

- `required_envs` (`list[str]`, `init=False`): Navnene på påkrevde miljøvariabler.
- `local` (`bool`, `init=False`): Om kjøringen er lokal.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`set_and_check_envs()`](functions.md#set_and_check_envs) | Oppretter instans |

**Metoder**

<a id="post_init"></a>
#### `__post_init__()`

Fyller listen over nødvendige variabler, avgjør om kjøringen er lokal og kopierer lisensfilen når den kjører på Nais.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Dataklassens genererte `__init__()` | Kaller hook ved opprettelse fra set_and_check_envs() |

<a id="iter"></a>
#### `__iter__()`

Gjør listen over påkrevde miljøvariabler tilgjengelig ved iterasjon over objektet.

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| Ingen interne kallere | Kan kalles av Pythons iterasjonsprotokoll |

<a id="load_envs"></a>
#### `load_envs()`

Laster `OS.env` eller `UR.env` ved lokal kjøring. På Nais brukes allerede tilgjengelige miljøvariabler.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`set_and_check_envs()`](functions.md#set_and_check_envs) | Kaller |

<a id="check_envs"></a>
#### `check_envs()`

Kontrollerer at alle nødvendige miljøvariabler finnes; kaster [`EnvsNotSetError`](exceptions.md#envsnotseterror) hvis noen mangler.

**Returns:**

`None`: Ingen returverdi.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`set_and_check_envs()`](functions.md#set_and_check_envs) | Kaller |

<a id="copy_db2_license"></a>
#### `_copy_db2_license()`

Kopierer DB2-lisensfilen til Python-miljøet i Nais. Mangler filen, kastes [`Db2LicenseNotFoundError`](exceptions.md#db2licensenotfounderror).

**Returns:**

Returtype ikke annotert.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`__post_init__()`](#post_init) | Kaller |

<a id="get_required_envs"></a>
#### `_get_required_envs()`

Returnerer navnene på nødvendige DB2- og GCP-miljøvariabler.

**Returns:**

`list[str]`: Navnene på påkrevde miljøvariabler.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`__post_init__()`](#post_init) | Kaller |

<a id="set_local"></a>
#### `_set_local()`

Sjekker om `NAIS_CLUSTER_NAME` mangler; hvis ja, regnes kjøringen som lokal.

**Returns:**

`bool`: Om programmet kjører lokalt.

**Kalles av**

| Kaller | Relasjon |
| --- | --- |
| [`__post_init__()`](#post_init) | Kaller |

[Til oversikten](../index.md)
