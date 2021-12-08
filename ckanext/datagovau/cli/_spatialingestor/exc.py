from __future__ import annotations

import logging
from typing import NoReturn

log = logging.getLogger(__name__)


class IngestionException(Exception):
    pass


class BadConfig(IngestionException):
    pass


class IngestionFail(IngestionException):
    pass


def fail(reason: str) -> NoReturn:
    log.error(reason)
    raise IngestionFail(reason)
