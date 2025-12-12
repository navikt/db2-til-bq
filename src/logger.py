import logging
import datetime


class Logger:
    def __init__(self, name: str, log_to_file: bool = False, level: int = logging.DEBUG):
        self._name = name
        self._log_to_file = log_to_file
        self._level = level

        self._logger = None
        self._configure_logger()


    def _log(self, message: str, level: int ) -> None:
        return self._logger.log(level, message)

    def debug(self, message: str) -> None:
        return self._log(message=message, level=logging.DEBUG)

    def info(self, message: str) -> None:
        return self._log(message=message, level=logging.INFO)

    def warning(self, message: str) -> None:
        return self._log(message=message, level=logging.WARNING)

    def error(self, message: str) -> None:
        return self._log(message=message, level=logging.ERROR)

    def critical(self, message: str) -> None:
        return self._log(message=message, level=logging.CRITICAL)


    def _configure_logger(self) -> None:
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
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)