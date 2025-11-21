from src.class_table import Table

tables = [
    Table(
        name="t_vent_beregning",
        table_type="fak",
        columns=["beregnings_id", "dato_beregnet", "kode_faggruppe", "tidspkt_reg"],
        check_col="tidspkt_reg",
        col_descriptions=[
            "Unik identifikator for en utført beregning i Oppdrag Z.",
            "Datoen beregningen er utført.",
            "Kode for gruppering av fagområder med felles egenskaper, f.eks. 'PEN' for pensjonsrelaterte ytelser",
            "Tidspunktet for når raden er satt inn i tabellen.",
        ],
    ),
    Table(
        name="t_vent_stoppnivaa",
        table_type="fak",
        columns=[
            "beregnings_id ",
            "stoppnivaa_id ",
            "oppdrags_id ",
            "fagsystem_id ",
            "type_skatt ",
            "Kode_fagomraade ",
            "dato_periode_fom ",
            "dato_periode_tom ",
            "dato_forfall ",
            "dato_overfores ",
            "tidspkt_reg",
        ],
        col_descriptions=[
            "Unik identifikator for en utført beregning i Oppdrag Z.",
            "En beregning brytes ned i f.eks. perioder med tilhørende forfallsdato. Stoppnivå er et begrep i Oppdrag Z bruker for en slik nødvendig nebrytning. Stoppnivå id samt beregnings id unikt identifiserer et stoppnivå fra en beregning.",
            "Identifikator på utbetalingsoppdraget, unik identifikator som oppdrag Z oppretter når man mottar ett vedtak til utbetaling fra fagsystem.",
            "Identifikator fra vedtaksløsningen som har avlevert ett utbetalingsoppdrag",
            "Angir om det er beregnet skatt med prosenttrekk eller tabelltrekk. Med denne kan man oppdage feil hvis skattekort ikke har kunnet blitt innhentet (skjer ikke ofte, men har skjedd) ",
            "En kode som angir selve ytelsen som beregningen gjelder, f.eks. 'AAP' for arbeidsavklaringspenger",
            "Startsdatoen stoppnivået gjelder for. Sammen med dato_periode_tom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp ",
            "Sluttdatoen stoppnivået gjelder for. Sammen med dato_periode_fom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp ",
            "Dato for når ytelse skal utbetales.",
            "Antakeligvis når ytelse overføres til bank?",
            "Tidspunktet for når raden er satt inn i tabellen.",
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_vent_stoppstatus",
        table_type="fak",
        columns=[
            "beregnings_id",
            "stoppnivaa_id",
            "kode_ventestatus",
            "Lopenr",
            "tidspkt_reg",
        ],
        col_descriptions=[
            "Unik identifikator for en utført beregning i Oppdrag Z.",
            "En beregning brytes ned i f.eks. perioder med tilhørende forfallsdato. Stoppnivå er et begrep i Oppdrag Z bruker for en slik nødvendig nebrytning. Stoppnivå id samt beregnings id unikt identifiserer et stoppnivå fra en beregning.",
            "Identifikator for status på stoppnivået i venteregisteret, eks OVFO (for 'Overført til UR')",
            "Tall som er >=1, hopper med steg += 1, 9999 markerer gjeldende ventestatus",
            "Tidspunktet ventestatusen er registert. Når siste statusrad settes til lopenr = 9999, så oppdateres også statusen på tidligere rad som hadde gjeldende ventestatus, men oppdaterer ikke tidspkt_reg for den tidligere gjeldende statusraden (i.e. tidspkt_reg har kun insert logikk).",
        ],
        check_col="tidspkt_reg",
    ),
    Table(
        name="t_faggruppe",
        table_type="dim",
        columns=["kode_faggruppe", "navn_faggruppe", "tidspkt_reg"],
        check_col="tidspkt_reg",
        col_descriptions=[
            "Kode for gruppering av fagområder med felles egenskaper, f.eks. 'PEN' for pensjonsrelaterte ytelser",
            "Navn/beskrivelse på faggruppe",
            "Tidspunktet for når raden er satt inn i tabellen.",
        ],
    ),
    Table(
        name="t_fagomraade",
        table_type="dim",
        columns=["kode_fagomraade", "navn_fagomraade", "kode_faggruppe", "tidspkt_reg"],
        check_col="tidspkt_reg",
        col_descriptions=[
            "En kode som angir selve ytelsen som beregningen gjeldre, f.eks. 'AAP' for arbeidsavklaringspenger",
            "Navn/beskrivelse på fagområde",
            "Kode for gruppering av fagområder med felles egenskaper, f.eks. 'PEN' for pensjonsrelaterte ytelser",
            "Tidspunktet for når raden er satt inn i tabellen.",
        ],
    ),
    Table(
        name="t_vent_statuskode",
        table_type="dim",
        columns=["kode_ventestatus", "beskrivelse", "tidspkt_reg"],
        check_col="tidspkt_reg",
        col_descriptions=[
            "Identifikator for status på stoppnivået i venteregisteret, eks OVFO (for 'Overført til UR')",
            "Beskrivelse av ventestatuskode, eks 'Overført til UR'",
            "Tidspunktet for når raden er satt inn i tabellen.",
        ],
    ),
]

if __name__ == "__main__":
    schema = "OS231Q2"

    for table in tables:
        print(table.name)
        print(table.build_sql(schema=schema, maxval_tgt=5))
        if table.col_descriptions:
            print("there are some descs here")
