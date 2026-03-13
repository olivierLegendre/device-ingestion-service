class IngestionError(Exception):
    """Base class for ingestion-specific domain errors."""


class UnsupportedProtocolError(IngestionError):
    """Raised when no parser can handle a protocol."""


class InvalidPayloadError(IngestionError):
    """Raised when payload shape is invalid."""


class UnprocessablePayloadError(IngestionError):
    """Raised when payload has no ingestible measurement."""
