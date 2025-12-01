from src.class_table import Table, TableType
from google.cloud.bigquery import SchemaField

tables = [
    Table(
        name="t_vent_beregning",
        description="Beregninger i Oppdragssystemet",
        table_type=TableType.FAK,
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Unik identifikator for beregninger i Oppdragssystemet.",
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
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_vent_stoppnivaa",
        description="""
        Stoppnivå er et begrep i Oppdragssystemet som brukes for nedbryting av beregninger.
        En beregning kan brytes ned i f.eks. perioder med tilhørende forfallsdato, gjelde forskjellige mottakere, eller kan gjelde ulike saker (f.eks sykepenger til én bruker, men gjelder flere forhold). 
        Stoppnivå id sammen med beregnings id vil unikt identifisere et stoppnivå for en beregning""",
        table_type=TableType.FAK,
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Identifikator for beregninger i Oppdragssystemet.",
            ),
            SchemaField(
                name="stoppnivaa_id",
                field_type="INTEGER",
                description="Identifikator for stoppnivå i Oppdragssystemet. Stoppnivaa_id sammen med beregnings_id vil unikt identifisere et stoppnivå for en beregning. Ikke nødvendigvis inkrementelt med 1.",
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
            ),
            SchemaField(
                name="type_skatt",
                field_type="STRING",
                description="Angir om det er beregnet skatt med prosenttrekk eller tabelltrekk.",
            ),
            SchemaField(
                name="kode_fagomraade",
                field_type="STRING",
                description="En kode som angir selve ytelsen som beregningen gjelder, f.eks. 'AAP' for arbeidsavklaringspenger",
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
    Table(
        name="t_vent_stoppstatus",
        description="Status for stoppnivå. Stoppstatus er synonymt med ventestatus.",
        table_type=TableType.FAK,
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Identifikator for beregninger i Oppdragssystemet.",
            ),
            SchemaField(
                name="stoppnivaa_id",
                field_type="INTEGER",
                description="Identifikator for stoppnivå i Oppdragssystemet",
            ),
            SchemaField(
                name="kode_ventestatus",
                field_type="STRING",
                description="Kode for status på stoppnivået, f.eks OVFO (for 'Overført til UR')",
            ),
            SchemaField(
                name="lopenr",
                field_type="INTEGER",
                description="Løpenummer. Tall som er >=1, hopper med steg += 1, 9999 markerer gjeldende ventestatus",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_faggruppe",
        description="Kodeverk for faggruppe. En faggruppe består av flere fagområder hvor man ønsker en samlet felles skatt- og trekkberegning. F.eks. pensjonsrelaterte ytelser",
        table_type=TableType.DIM,
        cols=[
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for faggruppe. F.eks. 'PEN' for pensjonsrelaterte ytelser",
            ),
            SchemaField(
                name="navn_faggruppe",
                field_type="STRING",
                description="Navn/beskrivelse på faggruppe",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_fagomraade",
        description="Kodeverk for fagområde. Fagområde angir selve ytelsen, f.eks. arbeidsavklaringspenger",
        table_type=TableType.DIM,
        cols=[
            SchemaField(
                name="kode_fagomraade",
                field_type="STRING",
                description="Kode for fagområde. F.eks. 'AAP' for arbeidsavklaringspenger",
            ),
            SchemaField(
                name="navn_fagomraade",
                field_type="STRING",
                description="Navn/beskrivelse på fagområde",
            ),
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for faggruppe, f.eks. 'ARBYT' for arbeidsrelaterte ytelser",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_vent_statuskode",
        description="Kodeverk for ventestatus/stoppstatus. En ventestatus angir statusen på et stoppnivå i Oppdragssystemet",
        table_type=TableType.DIM,
        cols=[
            SchemaField(
                name="kode_ventestatus",
                field_type="STRING",
                description="Kode for ventestatus. F.eks OVFO (for 'Overført til UR')",
            ),
            SchemaField(
                name="beskrivelse",
                field_type="STRING",
                description="Beskrivelse av ventestatuskode, eks 'Overført til UR'",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er lastet inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
]

if __name__ == "__main__":
    schema = "OS231Q2"

    for table in tables:
        print(table.name)

        print(table.build_sql(schema=schema, load_method="delta"))
