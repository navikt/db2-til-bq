from src.class_table import Table

tables = [
    Table(name = "t_vent_beregning", columns = ['beregnings_id', 'dato_beregnet', 'kode_faggruppe', 'tidspkt_reg'], check_col='tidspkt_reg', load_method='delta'),
    Table(name = "t_vent_stoppnivaa", columns = ['beregnings_id ',
                                                'stoppnivaa_id ',
                                                'oppdrags_id ',
                                                'fagsystem_id ',
                                                'type_skatt ',
                                                'Kode_fagomraade ',
                                                'dato_periode_fom ',
                                                'dato_periode_tom ',
                                                'dato_forfall ',
                                                'dato_overfores ',
                                                'tidspkt_reg'],
                                    col_descriptions=['for kobling ',
                                                'for kobling. En beregning brytes ned i perioder med tilførende forfalls dato. Stopp nivå ID er en identifikator for de ulike periodene ',
                                                'for å spore til et oppdrag (muligens ikke så viktig i første omgang, men er fort noe vi kommer til å få bruk for) ',
                                                'spore tilbake beregningen til vedtaksløsning (muligens ikke så viktig i første omgang, men er fort noe vi kommer til å få bruk for) ',
                                                'Angir om det er beregnet skatt med prosenttrekk eller tabelltrekk. Med denne kan man oppdage feil hvis skattekort ikke har kunnet blitt innhentet (skjer ikke ofte, men har skjedd) ',
                                                'filtrere beregninger på fagområde (og faggruppe) ',
                                                'startsdatoen stoppnivået gjelder for. Sammen med dato_periode_tom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp ',
                                                'startsdatoen stoppnivået gjelder for. Sammen med dato_periode_fom er utgjør dette perioden det gjelder for. For statistikk og forventningsstyring av utbetalingsforløp ',
                                                'dato for når ytelse skal utbetales. For statistikk og forventningsstyring av utbetalingsforløp ',
                                                'antakeligvis når ytelse overføres til bank? For statistikk og forventningsstyring av utbetalingsforløp ',
                                                ],
                                    check_col='tidspkt_reg', load_method='delta'),
    Table(name='t_vent_stoppstatus', columns=['beregnings_id','stoppnivaa_id','kode_ventestatus','Lopenr','tidspkt_reg'],
          col_descriptions=['kobling', 'kobling', 'id for ventestatus', 
                            'tallet er >=1, hopper med steg += 1, 9999 for gjeldendeventestatus',
                            'tidspunktet ventestatusen er registert. Når siste statusrad settes til lopenr = 9999, så oppdateres også statusen på tidligere rad som hadde gjeldende ventestatus, men oppdatere ikke tidspkt_reg for den tidligere gjeldende statusraden (i.e. tidspkt_reg har kun insert logikk).',
                            ],
                            check_col='tidspkt_reg', load_method='delta'),

]

static_tables = [
    Table(name = "t_faggruppe", columns = ['kode_faggruppe', 'navn_faggruppe', 'tidspkt_reg'], check_col='tidspkt_reg', load_method='full'),
    Table(name = "t_fagomraade", columns = ['kode_fagomraade', 'navn_fagomraade', 'kode_faggruppe', 'tidspkt_reg'], check_col='tidspkt_reg', load_method='full'),
    Table(name='t_vent_statuskode', columns=['kode_ventestatus', 'beskrivelse', 'tidspkt_reg'], check_col='tidspkt_reg', load_method='full')
]

if __name__ == "__main__":
    schema = "OS231Q2"

    for table in tables:
        print(table.name)
        print(table.build_sql(schema=schema, maxval_tgt=5))
        if table.col_descriptions:
            print("there are some descs here")