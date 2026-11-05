from typing import Any, Union


class EnvsNotSetError(BaseException):
     """Feil som signaliserer at påkrevde miljøvariabler mangler."""

     def __init__(self, message: str) -> None:
         """Opprett feilen med en melding.

         Parameters
         ----------
         message : str
             Feilmeldingen som videresendes til ``BaseException``.
         """
         super().__init__(message)

class Db2LicenseNotFoundError(BaseException):
    """Feil som signaliserer at DB2-lisensfilen ikke ble funnet."""

    def __init__(self, message: str) -> None:
        """Opprett feilen med en melding.

        Parameters
        ----------
        message : str
            Feilmeldingen som videresendes til ``BaseException``.
        """
        super().__init__(message)


class BigQueryError(BaseException):
    """Generell feil rapportert av BigQuery."""

    def __init__(self, message: str) -> None:
        """Opprett feilen med en melding.

        Parameters
        ----------
        message : str
            Feilmeldingen som videresendes til ``BaseException``.
        """
        super().__init__(message)


class BigQueryInvalidValueError(BaseException):
    """BigQuery-feil knyttet til en ugyldig kolonneverdi."""

    def __init__(self, message: str) -> None:
        """Opprett feilen med en melding.

        Parameters
        ----------
        message : str
            Feilmeldingen som videresendes til ``BaseException``.
        """
        super().__init__(message)


class BigQueryMissingRequiredError(BaseException):
    """BigQuery-feil knyttet til en manglende påkrevd verdi."""

    def __init__(self, message: str) -> None:
        """Opprett feilen med en melding.

        Parameters
        ----------
        message : str
            Feilmeldingen som videresendes til ``BaseException``.
        """
        super().__init__(message)


class BigQueryErrors:
    """Klassifiser BigQuery-feilmeldinger til relevante unntak.

    Attributes
    ----------
    _errors : Any
        Feillistene som mottas fra BigQuery.
    _exceptions : list of BaseException
        Feilobjektene som ble opprettet fra meldingene.
    """

    def __init__(self, errors: Union[list[dict[str,str]], Any]) -> None:
        """Lagre feillisten og opprett klassifiserte unntak.

        Parameters
        ----------
        errors : Any
            Feil fra BigQuery, iterert som meldingsordbøker.
        """
        self._errors = errors
        self._exceptions: list[Union[BigQueryInvalidValueError, BigQueryMissingRequiredError, BigQueryError]] = []
        self._process_errors()


    def _process_errors(self):
        """Klassifiser hver melding og legg unntaket til i feillisten.

        Melding som slutter med ``null`` klassifiseres som manglende påkrevd
        verdi. Melding der nest siste ord er ``column`` klassifiseres som
        ugyldig verdi; øvrige meldinger blir generelle BigQuery-feil.
        """
        for error in self._errors:
            message = error.get("message")
            tokens = message.split(" ")

            if tokens[-1] == "null":
                self._exceptions.append(BigQueryMissingRequiredError(message=message))

            elif tokens[-2] == "column":
                self._exceptions.append(BigQueryInvalidValueError(message=message))

            else:
                self._exceptions.append(BigQueryError(message=message))

    def __iter__(self):
        """Iterer over de klassifiserte BigQuery-unntakene.

        Yields
        ------
        BaseException
            Neste unntak som ble opprettet fra feillisten.
        """
        for exception in self._exceptions:
            yield exception