"""SendGrid tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_sendgrid.streams import (
    BouncesStream,
    SendGridStream, 
    AutomationIdsStream,
    EmailActivitySteam,
    AutomationStatsStream,
    StatsByBrowserStream,
    StatsByClientStream,
    StatsByCountryStream,
    StatsByDevicesStream,
    StatsByMailboxProviderStream,
)


class TapSendGrid(Tap):
    """SendGrid tap class."""

    name = "tap-sendgrid"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "from_email",
            th.StringType,
            description="The email address that was used as sender",
        ),
        th.Property(
            "start_datetime",
            th.DateTimeType,
            description="The earliest record date to sync",
            default="1970-02-01T00:00:00Z",
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The email address that was used as sender",
            default=500,
        ),
    ).to_dict()

    def discover_streams(self) -> list[SendGridStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            AutomationIdsStream(self, paginator_type="offset"),
            AutomationStatsStream(self, paginator_type="offset"),
            BouncesStream(self, paginator_type="offset"),
            EmailActivitySteam(self, paginator_type="base"),
            StatsByBrowserStream(self, paginator_type='offset'),
            StatsByClientStream(self, paginator_type='offset'),
            StatsByCountryStream(self, paginator_type='offset'),
            StatsByDevicesStream(self, paginator_type='offset'),
            StatsByMailboxProviderStream(self, paginator_type='offset'),
        ]


if __name__ == "__main__":
    TapSendGrid.cli()
