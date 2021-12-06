class IngestionException(Exception):
    pass


class BadConfig(IngestionException):
    pass


class IngestionFail(IngestionException):
    pass
