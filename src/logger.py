import logging
import datetime


class Logger:
    """Konfigurer og bruk en navngitt Python-logger.

    Loggeren sender meldinger til standardfeil og kan i tillegg skrive til
    en tidsstemplet loggfil.

    Parameters
    ----------
    name : str
        Navn brukt til å hente den underliggende Python-loggeren.
    log_to_file : bool, default=False
        Om meldinger også skal skrives til en loggfil i gjeldende katalog.
    level : int, default=logging.DEBUG
        Laveste loggnivå som skal behandles.

    Attributes
    ----------
    _name : str
        Navnet på den underliggende loggeren.
    _log_to_file : bool
        Angir om filutdata er aktivert.
    _level : int
        Konfigurert loggnivå.
    _logger : logging.Logger
        Underliggende logger fra standardbiblioteket.
    """

    def __init__(self, name: str, log_to_file: bool = False, level: int = logging.DEBUG):
        """Opprett loggeren og konfigurer dens håndterere.

        Parameters
        ----------
        name : str
            Navn brukt til å hente den underliggende Python-loggeren.
        log_to_file : bool, default=False
            Om meldinger også skal skrives til en tidsstemplet loggfil.
        level : int, default=logging.DEBUG
            Laveste loggnivå som skal behandles.

        Returns
        -------
        None
            Konstruktøren returnerer ikke en verdi.
        """
        self._name = name
        self._log_to_file = log_to_file
        self._level = level

        self._logger = None
        self._configure_logger()


    def _log(self, message: str, level: int ) -> None:
        """Send en melding til den underliggende loggeren på angitt nivå.

        Parameters
        ----------
        message : str
            Meldingen som skal logges.
        level : int
            Loggnivå fra ``logging``.

        Returns
        -------
        None
            Returnerer resultatet fra ``logging.Logger.log``.
        """
        return self._logger.log(level, message)

    def debug(self, message: str) -> None:
        """Logg en feilsøkingsmelding.

        Parameters
        ----------
        message : str
            Meldingen som skal logges.

        Returns
        -------
        None
            Metoden returnerer ikke en verdi.
        """
        return self._log(message=message, level=logging.DEBUG)

    def info(self, message: str) -> None:
        """Logg en informasjonsmelding.

        Parameters
        ----------
        message : str
            Meldingen som skal logges.

        Returns
        -------
        None
            Metoden returnerer ikke en verdi.
        """
        return self._log(message=message, level=logging.INFO)

    def warning(self, message: str) -> None:
        """Logg en advarsel.

        Parameters
        ----------
        message : str
            Meldingen som skal logges.

        Returns
        -------
        None
            Metoden returnerer ikke en verdi.
        """
        return self._log(message=message, level=logging.WARNING)

    def error(self, message: str) -> None:
        """Logg en feilmelding.

        Parameters
        ----------
        message : str
            Meldingen som skal logges.

        Returns
        -------
        None
            Metoden returnerer ikke en verdi.
        """
        return self._log(message=message, level=logging.ERROR)

    def critical(self, message: str) -> None:
        """Logg en kritisk melding.

        Parameters
        ----------
        message : str
            Meldingen som skal logges.

        Returns
        -------
        None
            Metoden returnerer ikke en verdi.
        """
        return self._log(message=message, level=logging.CRITICAL)


    def _configure_logger(self) -> None:
        """Sett opp strøm- og eventuelt filhåndterer på loggeren.

        Strømmen formateres med ``StreamFormatter``. Filhåndtereren bruker
        tidsstempel i filnavnet og en standard tekstformatterer.

        Returns
        -------
        None
            Metoden returnerer ikke en verdi.

        Notes
        -----
        Logger-objekter med samme navn hentes fra standardbibliotekets globale
        register. Et nytt kall legger til nye håndterere på dette objektet.
        """
        self._logger: logging.Logger = logging.getLogger(self._name)

        stream_formatter = StreamFormatter()
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(stream_formatter)

        self._logger.addHandler(stream_handler)

        if self._log_to_file:
            current_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            file_handler = logging.FileHandler(f"{self._name}-{current_timestamp}.log")
            file_formatter = logging.Formatter("%(levelname)s: %(asctime)s: %(name)s: %(message)s")
            file_handler.setFormatter(file_formatter)

            self._logger.addHandler(file_handler)


        self._logger.propagate = False
        self._logger.setLevel(self._level)


class StreamFormatter(logging.Formatter):
    """Formater loggposter med nivåavhengige ANSI-farger.

    Attributes
    ----------
    white : str
        ANSI-kode for hvit tekst.
    yellow : str
        ANSI-kode for gul tekst.
    green : str
        ANSI-kode for grønn tekst.
    red : str
        ANSI-kode for rød tekst.
    bold_red : str
        ANSI-kode brukt for kritiske meldinger.
    reset : str
        ANSI-kode som tilbakestiller tekstformatet.
    format : str
        Formatstreng for loggposter.
    FORMATS : dict
        Formatstrenger indeksert etter loggnivå.
    """

    white = "\x1b[1;37m"
    yellow = "\x1b[1;33m"
    green = "\x1b[1;32m"
    red = "\x1b[1;31m"
    bold_red = "\x1b[1;31m"
    reset = "\x1b[0m"
    format = "%(levelname)s: %(asctime)s: %(name)s: %(message)s"

    FORMATS = {
        logging.DEBUG: green + format + reset,
        logging.INFO: white + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        """Formater en loggpost med formatet for postens nivå.

        Parameters
        ----------
        record : logging.LogRecord
            Loggposten som skal formateres.

        Returns
        -------
        str
            Den formaterte loggteksten.

        Notes
        -----
        Ukjente nivåer gir ingen formatstreng fra ``FORMATS`` og kan derfor
        føre til feil når standardformattereren opprettes.
        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)