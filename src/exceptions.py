from typing import Any, Union

class BigQueryError(BaseException):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class BigQueryInvalidValueError(BaseException):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class BigQueryMissingRequiredError(BaseException):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class BigQueryErrors:
    def __init__(self, errors: Union[list[dict[str,str]], Any]) -> None:
        self._errors = errors
        self._exceptions: list[Union[BigQueryInvalidValueError, BigQueryMissingRequiredError, BigQueryError]] = []
        self._process_errors()


    def _process_errors(self):
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
        for exception in self._exceptions:
            yield exception