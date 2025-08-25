from dataclasses import dataclass

@dataclass
class Table:
    """klasse for å lagre informasjon om kildetabellene våre"""
    name: str
    columns: list

    def build_sql(self, schema: str) -> str:
        query = f"""SELECT {', '.join(self.columns)} 
                FROM {schema}.{self.name}
        """
        return query


tables = [
    Table(name = "t_faggruppe", columns = ['kode_faggruppe', 'navn_faggruppe']),
    Table(name = "t_fagomraade", columns = ['kode_fagomraade', 'navn_fagomraade', 'kode_faggruppe']),

]


tables2 = [
    {
        "name": "t_faggruppe",
        "query":"select KODE_FAGGRUPPE, NAVN_FAGGRUPPE from t_faggruppe"
    },
    {
        "name": "t_fagomraade",
        "query":"select KODE_FAGOMRAADE ,NAVN_FAGOMRAADE,KODE_FAGGRUPPE from t_fagomraade"
    },
    {
        "name": "t_vent_beregning",
        "query":"select Beregnings_id, dato_beregnet, kode_faggruppe from t_vent_beregning"
    },
    {
        "name": "t_vent_stoppnivaa",
        "query":"""
                select
                Beregnings_id /* for kobling */
                ,stoppnivaa_id /* for kobling. En beregning brytes ned i perioder med tilførende forfalls dato. Stopp nivå ID er en identifikator for de ulike periodene */
                ,oppdrags_id /*  for å spore til et oppdrag (muligens ikke så viktig i første omgang, men er fort noe vi kommer til å få bruk for) */
                ,fagsystem_id /* spore tilbake beregningen til vedtaksløsning (muligens ikke så viktig i første omgang, men er fort noe vi kommer til å få bruk for) */
                ,type_skatt /* Angir om det er beregnet skatt med prosenttrekk eller tabelltrekk. Med denne kan man oppdage feil hvis skattekort ikke har kunnet blitt innhentet (skjer ikke ofte, men har skjedd) */
                ,Kode_fagomraade /* filtrere beregninger på fagområde (og faggruppe) */
                ,dato_periode_fom /* startsdatoen stoppnivået  gjelder for. Sammen med dato_periode_tom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp */
                ,dato_periode_tom /* startsdatoen stoppnivået  gjelder for. Sammen med dato_periode_fom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp */
                ,dato_forfall /* dato for når ytelse skal utbetales. For statistikk og forventningsstyring av utbetalingsforløp */
                ,dato_overfores /* antakeligvis når ytelse overføres til bank? For statistikk og forventningsstyring av utbetalingsforløp */
                from T_VENT_STOPPNIVAA /* antall beregninger, per fagområde */
                """
    },
    {
        "name": "t_vent_stoppstatus",
        "query":"""
                select
                beregnings_id /* kobling */
                ,stoppnivaa_id /* kobling */
                ,kode_ventestatus /* id for ventestatus */
                ,Lopenr /* tallet er >=1, hopper med steg += 1, 9999 for gjeldendeventestatus */
                ,Tidspkt_reg /* tidspunktet ventestatusen er registert. Når siste statusrad settes til lopenr = 9999, så oppdateres også statusen på tidligere rad som hadde gjeldende ventestatus, men oppdatere ikke tidspkt_reg for den tidligere gjeldende statusraden (i.e. tidspkt_reg har kun insert logikk). */
                from T_VENT_STOPPSTATUS /* oversikt over beregningsflyten og identifisere eventuelle flaskehalser */
                """
    },
    {
        "name": "t_vent_statuskode",
        "query":"""
                select
                Kode_ventestatus /* id for ventestatus */
                ,Beskrivelse /* beskrivelsen av ventestatus, for vanskelig å vite hva ventestatusen betyr uten beskrivelsen */
                from T_VENT_STATUSKODE /* betydning av ventestatuskoden */
                """
    },
]

if __name__ == "__main__":
    schema = "OS231Q2"

    for table in tables2:
        print(table.name)
        print(table.build_sql(schema=schema))