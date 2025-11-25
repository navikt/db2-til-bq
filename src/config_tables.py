from src.class_table import Table, TableType
from google.cloud.bigquery import SchemaField

tables = [
    Table(
        name="t_vent_beregning",
        table_type=TableType.FAK,
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Unik identifikator for en utført beregning i Oppdrag Z.",
            ),
            SchemaField(
                name="dato_beregnet",
                field_type="DATE",
                description="Datoen beregningen er utført.",
            ),
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for gruppering av fagområder med felles egenskaper, f.eks. 'PEN' for pensjonsrelaterte ytelser",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er satt inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_vent_stoppnivaa",
        table_type=TableType.FAK,
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Unik identifikator for en utført beregning i Oppdrag Z.",
            ),
            SchemaField(
                name="stoppnivaa_id",
                field_type="INTEGER",
                description="En beregning brytes ned i f.eks. perioder med tilhørende forfallsdato. Stoppnivå er et begrep i Oppdrag Z bruker for en slik nødvendig nebrytning. Stoppnivå id samt beregnings id unikt identifiserer et stoppnivå fra en beregning.",
            ),
            SchemaField(
                name="oppdrags_id",
                field_type="INTEGER",
                description="Identifikator på utbetalingsoppdraget, unik identifikator som oppdrag Z oppretter når man mottar ett vedtak til utbetaling fra fagsystem.",
            ),
            SchemaField(
                name="fagsystem_id",
                field_type="STRING",
                description="Identifikator fra vedtaksløsningen som har avlevert ett utbetalingsoppdrag",
            ),
            SchemaField(
                name="type_skatt",
                field_type="STRING",
                description="Angir om det er beregnet skatt med prosenttrekk eller tabelltrekk. Med denne kan man oppdage feil hvis skattekort ikke har kunnet blitt innhentet (skjer ikke ofte, men har skjedd) ",
            ),
            SchemaField(
                name="kode_fagomraade",
                field_type="STRING",
                description="En kode som angir selve ytelsen som beregningen gjelder, f.eks. 'AAP' for arbeidsavklaringspenger",
            ),
            SchemaField(
                name="dato_periode_fom",
                field_type="DATE",
                description="Startsdatoen stoppnivået gjelder for. Sammen med dato_periode_tom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp ",
            ),
            SchemaField(
                name="dato_periode_tom",
                field_type="DATE",
                description="Sluttdatoen stoppnivået gjelder for. Sammen med dato_periode_fom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp ",
            ),
            SchemaField(
                name="dato_forfall",
                field_type="DATE",
                description="Dato for når ytelse skal utbetales.",
            ),
            SchemaField(
                name="dato_overfores",
                field_type="DATE",
                description="Antakeligvis når ytelse overføres til bank?",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er satt inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_vent_stoppstatus",
        table_type=TableType.FAK,
        cols=[
            SchemaField(
                name="beregnings_id",
                field_type="INTEGER",
                description="Unik identifikator for en utført beregning i Oppdrag Z.",
            ),
            SchemaField(
                name="stoppnivaa_id",
                field_type="INTEGER",
                description="En beregning brytes ned i f.eks. perioder med tilhørende forfallsdato. Stoppnivå er et begrep i Oppdrag Z bruker for en slik nødvendig nebrytning. Stoppnivå id samt beregnings id unikt identifiserer et stoppnivå fra en beregning.",
            ),
            SchemaField(
                name="kode_ventestatus",
                field_type="STRING",
                description="Identifikator for status på stoppnivået i venteregisteret, eks OVFO (for 'Overført til UR')",
            ),
            SchemaField(
                name="lopenr",
                field_type="INTEGER",
                description="Tall som er >=1, hopper med steg += 1, 9999 markerer gjeldende ventestatus",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet ventestatusen er registert. Når siste statusrad settes til lopenr = 9999, så oppdateres også statusen på tidligere rad som hadde gjeldende ventestatus, men oppdaterer ikke tidspkt_reg for den tidligere gjeldende statusraden (i.e. tidspkt_reg har kun insert logikk).",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_faggruppe",
        table_type=TableType.DIM,
        cols=[
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for gruppering av fagområder med felles egenskaper, f.eks. 'PEN' for pensjonsrelaterte ytelser",
            ),
            SchemaField(
                name="navn_faggruppe",
                field_type="STRING",
                description="Navn/beskrivelse på faggruppe",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er satt inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_fagomraade",
        table_type=TableType.DIM,
        cols=[
            SchemaField(
                name="kode_fagomraade",
                field_type="STRING",
                description="En kode som angir selve ytelsen som beregningen gjeldre, f.eks. 'AAP' for arbeidsavklaringspenger",
            ),
            SchemaField(
                name="navn_fagomraade",
                field_type="STRING",
                description="Navn/beskrivelse på fagområde",
            ),
            SchemaField(
                name="kode_faggruppe",
                field_type="STRING",
                description="Kode for gruppering av fagområder med felles egenskaper, f.eks. 'PEN' for pensjonsrelaterte ytelser",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er satt inn i tabellen.",
            ),
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_vent_statuskode",
        table_type=TableType.DIM,
        cols=[
            SchemaField(
                name="kode_ventestatus",
                field_type="STRING",
                description="Identifikator for status på stoppnivået i venteregisteret, eks OVFO (for 'Overført til UR')",
            ),
            SchemaField(
                name="beskrivelse",
                field_type="STRING",
                description="Beskrivelse av ventestatuskode, eks 'Overført til UR'",
            ),
            SchemaField(
                name="tidspkt_reg",
                field_type="DATETIME",
                description="Tidspunktet for når raden er satt inn i tabellen.",
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
