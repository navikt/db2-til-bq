from src.class_table import FakTable, DimTable
from google.cloud.bigquery import SchemaField

tables = [
    FakTable(
        name="t_vent_beregning",
        description="Beregninger i Oppdragssystemet",
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Unik identifikator for beregninger i Oppdragssystemet.",
                mode="REQUIRED",
            ),
            SchemaField(
                name="dato_beregnet",
                field_type="DATE",
                description="Datoen beregningen er utført i Oppdragssystemet.",
            ),
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for faggruppe. En faggruppe består av flere fagområder hvor man ønsker en samlet felles skatt- og trekkberegning. F.eks. 'PEN' for pensjonsrelaterte ytelser",
                max_length=8,
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    FakTable(
        name="t_vent_stoppnivaa",
        description="""
        Stoppnivå er et begrep i Oppdragssystemet som brukes for nedbryting av beregninger.
        En beregning kan brytes ned i f.eks. perioder med tilhørende forfallsdato, gjelde forskjellige mottakere, eller kan gjelde ulike saker (f.eks sykepenger til én bruker, men gjelder flere forhold). 
        Stoppnivå id sammen med beregnings id vil unikt identifisere et stoppnivå for en beregning""",
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Identifikator for beregninger i Oppdragssystemet.",
                mode="REQUIRED",
            ),
            SchemaField(
                name="stoppnivaa_id",
                field_type="INTEGER",
                description="Identifikator for stoppnivå i Oppdragssystemet. Stoppnivaa_id sammen med beregnings_id vil unikt identifisere et stoppnivå for en beregning. Ikke nødvendigvis inkrementelt med 1.",
                mode="REQUIRED",
            ),
            SchemaField(
                name="oppdrags_id",
                field_type="INTEGER",
                description="Identifikator på utbetalingsoppdraget, unik identifikator som Oppdragssystemet oppretter når man mottar ett vedtak til utbetaling fra fagsystem.",
            ),
            SchemaField(
                name="fagsystem_id",
                field_type="STRING",
                description="Identifikator fra vedtaksløsningen som har avlevert ett utbetalingsoppdrag",
                max_length=30,
            ),
            SchemaField(
                name="type_skatt",
                description="Angir om det er beregnet skatt med prosenttrekk eller tabelltrekk.",
                field_type="STRING",
                max_length=4,
            ),
            SchemaField(
                name="kode_fagomraade",
                field_type="STRING",
                description="En kode som angir selve ytelsen som beregningen gjelder, f.eks. 'AAP' for arbeidsavklaringspenger",
                max_length=8,
            ),
            SchemaField(
                name="dato_periode_fom",
                field_type="DATE",
                description="Startdatoen stoppnivået gjelder for. Sammen med dato_periode_tom utgjør dette perioden stoppnivået gjelder for.",
            ),
            SchemaField(
                name="dato_periode_tom",
                field_type="DATE",
                description="Sluttdatoen stoppnivået gjelder for. Sammen med dato_periode_fom utgjør dette perioden stoppnivået gjelder for.",
            ),
            SchemaField(
                name="dato_forfall",
                field_type="DATE",
                description="Dato for når stoppnivået skal utbetales.",
            ),
            SchemaField(
                name="dato_overfores",
                field_type="DATE",
                description="Dato for når stoppnivået er planlagt overført til reskontro",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    FakTable(
        name="t_vent_stoppstatus",
        description="Status for stoppnivå. Stoppstatus er synonymt med ventestatus.",
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Identifikator for beregninger i Oppdragssystemet.",
                mode="REQUIRED",
            ),
            SchemaField(
                name="stoppnivaa_id",
                field_type="INTEGER",
                description="Identifikator for stoppnivå i Oppdragssystemet",
                mode="REQUIRED",
            ),
            SchemaField(
                name="kode_ventestatus",
                field_type="STRING",
                description="Kode for status på stoppnivået, f.eks OVFO (for 'Overført til UR')",
                max_length=4,
            ),
            SchemaField(
                name="lopenr",
                field_type="INTEGER",
                description="Løpenummer. Tall som er >=1, hopper med steg += 1, 9999 markerer gjeldende ventestatus",
                mode="REQUIRED",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    DimTable(
        name="t_faggruppe",
        description="Kodeverk for faggruppe. En faggruppe består av flere fagområder hvor man ønsker en samlet felles skatt- og trekkberegning. F.eks. pensjonsrelaterte ytelser",
        cols=[
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for faggruppe. F.eks. 'PEN' for pensjonsrelaterte ytelser",
                mode="REQUIRED",
                max_length=8,
            ),
            SchemaField(
                name="navn_faggruppe",
                field_type="STRING",
                description="Navn/beskrivelse på faggruppe",
                max_length=50,
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
    ),
    DimTable(
        name="t_fagomraade",
        description="Kodeverk for fagområde. Fagområde angir selve ytelsen, f.eks. arbeidsavklaringspenger",
        cols=[
            SchemaField(
                name="kode_fagomraade",
                field_type="STRING",
                description="Kode for fagområde. F.eks. 'AAP' for arbeidsavklaringspenger",
                mode="REQUIRED",
                max_length=8,
            ),
            SchemaField(
                name="navn_fagomraade",
                field_type="STRING",
                description="Navn/beskrivelse på fagområde",
                max_length=50,
            ),
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for faggruppe, f.eks. 'ARBYT' for arbeidsrelaterte ytelser",
                max_length=8,
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
    ),
    DimTable(
        name="t_vent_statuskode",
        description="Kodeverk for ventestatus/stoppstatus. En ventestatus angir statusen på et stoppnivå i Oppdragssystemet",
        cols=[
            SchemaField(
                name="kode_ventestatus",
                field_type="STRING",
                description="Kode for ventestatus. F.eks OVFO (for 'Overført til UR')",
                mode="REQUIRED",
                max_length=4,
            ),
            SchemaField(
                name="beskrivelse",
                field_type="STRING",
                description="Beskrivelse av ventestatuskode, eks 'Overført til UR'",
                max_length=40,
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
    ),
]
