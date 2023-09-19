"""Stream type classes for tap-sendgrid."""

from __future__ import annotations

import datetime
import logging
from typing import Iterable, Optional
from pendulum.parser import parse
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_sendgrid.client import SendGridStream

LOGGER = logging.getLogger(__name__)


class BouncesStream(SendGridStream):
    """Define custom stream."""

    name = "bounces"
    path = "/suppression/bounces"
    primary_keys = ["email"]
    replication_key = "created"
    schema = th.PropertiesList(
        th.Property("email", th.StringType),
        th.Property("created", th.IntegerType),
        th.Property("reason", th.StringType),
        th.Property("status", th.StringType),
        th.Property("error", th.StringType),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """

        page_size = self.page_size
        offset = 0
        start_time = self.get_starting_replication_key_value(context)
        if not start_time:
            start_time = self.get_unix_start_time

        while True:
            resp = self.conn.client.suppression.bounces.get(
                request_headers=self.headers,
                query_params={
                    "start_time": start_time,
                    "offset": offset,
                    "limit": page_size,
                },
            )

            yield from resp.to_dict

            if not self.paginator.has_more(resp):
                break

            offset = self.paginator.get_next(resp)


class EmailActivitySteam(SendGridStream):
    name = "email_activity"
    path = "/v3/messages"
    primary_keys = ["msg_id"]
    replication_key = "last_event_time"
    schema = th.PropertiesList(
        th.Property("from_email", th.StringType),
        th.Property("msg_id", th.StringType),
        th.Property("subject", th.StringType),
        th.Property("to_email", th.StringType),
        th.Property("status", th.StringType),
        th.Property("opens_count", th.IntegerType),
        th.Property("clicks_count", th.IntegerType),
        th.Property("last_event_time", th.DateTimeType),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        page_size = self.page_size
        from_email = self.config.get("from_email")
        start_time = self.get_starting_replication_key_value(context)
        if not start_time:
            start_time = self.config.get("start_datetime")
        end_time = self.end_time

        while True:
            query = (
                f'from_email="{from_email}" '
                f'AND last_event_time BETWEEN TIMESTAMP "{start_time}" '
                f'AND TIMESTAMP "{end_time}"'
            )
            self.logger.debug(f"Query: {query}")

            resp = self.conn.client.messages.get(
                request_headers=self.headers,
                query_params={"query": query, "limit": page_size},
            )

            yield from resp.to_dict["messages"]

            if not self.paginator.has_more(resp):
                break

            end_time = self.paginator.get_last(resp)

class StatsStream(SendGridStream):

    name = "stats"
    path = "/stats"
    primary_keys = ["date"] # type: ignore
    replication_key = "date" # type: ignore
    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("stats", th.ArrayType(
            th.ObjectType(
                th.Property('metrics', th.ObjectType(
                    th.Property('blocks', th.IntegerType),
                    th.Property('bounce_drops', th.IntegerType),
                    th.Property('bounces', th.IntegerType),
                    th.Property('clicks', th.IntegerType),
                    th.Property('deferred', th.IntegerType),
                    th.Property('delivered', th.IntegerType),
                    th.Property('invalid_emails', th.IntegerType),
                    th.Property('opens', th.IntegerType),
                    th.Property('processed', th.IntegerType),
                    th.Property('requests', th.IntegerType),
                    th.Property('spam_report_drops', th.IntegerType),
                    th.Property('spam_reports', th.IntegerType),
                    th.Property('unique_clicks', th.IntegerType),
                    th.Property('unique_opens', th.IntegerType),
                    th.Property('unsubscribe_drops', th.IntegerType),
                    th.Property('unsubscribes', th.IntegerType),
                ))
            )
        )),
    ).to_dict()  # type: ignore

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """

        page_size = self.page_size
        offset = 0
        start_date = self.get_starting_replication_key_value(context)
        if not start_date:
            start_date = datetime.datetime.strptime(
            self.config["start_datetime"], "%Y-%m-%dT%H:%M:%SZ"
        )
        else:
            start_date = parse(start_date)

        while True:
            resp = self.conn.client.stats.get(
                request_headers=self.headers,
                query_params={
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "offset": offset,
                    "limit": page_size,
                },
            ) # type: ignore
            yield from resp.to_dict # type: ignore

            if not self.paginator.has_more(resp): # type: ignore
                break 

            offset = self.paginator.get_next(resp) # type: ignore


class StatsByBrowserStream(SendGridStream):

    name = "stats_by_browser"
    path = "/browsers/stats"
    primary_keys = ["date"] # type: ignore
    replication_key = "date" # type: ignore
    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("stats", th.ArrayType(
            th.ObjectType(
                th.Property('type', th.StringType),
                th.Property('name', th.StringType),
                th.Property('metrics', th.ObjectType(
                    th.Property('clicks', th.IntegerType),
                    th.Property('unique_clicks', th.IntegerType),
                ))
            )
        )),
    ).to_dict()  # type: ignore

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """

        page_size = self.page_size
        offset = 0
        start_date = self.get_starting_replication_key_value(context)
        if not start_date:
            start_date = datetime.datetime.strptime(
            self.config["start_datetime"], "%Y-%m-%dT%H:%M:%SZ"
        )
        else:
            start_date = parse(start_date)


        while True:
            resp = self.conn.client.browsers.stats.get(
                request_headers=self.headers,
                query_params={
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "offset": offset,
                    "limit": page_size,
                },
            ) # type: ignore
            yield from resp.to_dict # type: ignore

            if not self.paginator.has_more(resp): # type: ignore
                break 

            offset = self.paginator.get_next(resp) # type: ignore

class StatsByClientStream(SendGridStream):

    name = "stats_by_client"
    path = "/clients/stats"
    primary_keys = ["date"] # type: ignore
    replication_key = "date" # type: ignore
    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("stats", th.ArrayType(
            th.ObjectType(
                th.Property('type', th.StringType),
                th.Property('name', th.StringType),
                th.Property('metrics', th.ObjectType(
                    th.Property('opens', th.IntegerType),
                    th.Property('unique_opens', th.IntegerType),
                ))
            )
        )),
    ).to_dict()  # type: ignore

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """

        page_size = self.page_size
        offset = 0
        start_date = self.get_starting_replication_key_value(context)
        if not start_date:
            start_date = datetime.datetime.strptime(
            self.config["start_datetime"], "%Y-%m-%dT%H:%M:%SZ"
        )
        else:
            start_date = parse(start_date)

        while True:
            resp = self.conn.client.clients.stats.get(
                request_headers=self.headers,
                query_params={
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "offset": offset,
                    "limit": page_size,
                },
            ) # type: ignore
            yield from resp.to_dict # type: ignore

            if not self.paginator.has_more(resp): # type: ignore
                break 

            offset = self.paginator.get_next(resp) # type: ignore

class StatsByCountryStream(SendGridStream):

    name = "stats_by_country"
    path = "/geo/stats"
    primary_keys = ["date"] # type: ignore
    replication_key = "date" # type: ignore
    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("stats", th.ArrayType(
            th.ObjectType(
                th.Property('type', th.StringType),
                th.Property('name', th.StringType),
                th.Property('metrics', th.ObjectType(
                    th.Property('clicks', th.IntegerType),
                    th.Property('unique_clicks', th.IntegerType),
                    th.Property('opens', th.IntegerType),
                    th.Property('unique_opens', th.IntegerType),
                ))
            )
        )),
    ).to_dict()  # type: ignore

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """

        page_size = self.page_size
        offset = 0
        start_date = self.get_starting_replication_key_value(context)
        if not start_date:
            start_date = datetime.datetime.strptime(
            self.config["start_datetime"], "%Y-%m-%dT%H:%M:%SZ"
        )
        else:
            start_date = parse(start_date)


        while True:
            resp = self.conn.client.geo.stats.get(
                request_headers=self.headers,
                query_params={
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "offset": offset,
                    "limit": page_size,
                },
            ) # type: ignore
            yield from resp.to_dict # type: ignore

            if not self.paginator.has_more(resp): # type: ignore
                break 

            offset = self.paginator.get_next(resp) # type: ignore

class StatsByDevicesStream(SendGridStream):
    name = "stats_by_devices"
    path = "/devices/stats"
    primary_keys = ["date"] # type: ignore
    replication_key = "date" # type: ignore
    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("stats", th.ArrayType(
            th.ObjectType(
                th.Property('type', th.StringType),
                th.Property('name', th.StringType),
                th.Property('metrics', th.ObjectType(
                    th.Property('opens', th.IntegerType),
                    th.Property('unique_opens', th.IntegerType),
                ))
            )
        )),
    ).to_dict()  # type: ignore

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """

        page_size = self.page_size
        offset = 0
        start_date = self.get_starting_replication_key_value(context)
        if not start_date:
            start_date = datetime.datetime.strptime(
            self.config["start_datetime"], "%Y-%m-%dT%H:%M:%SZ"
        )
        else:
            start_date = parse(start_date)

        while True:
            resp = self.conn.client.devices.stats.get(
                request_headers=self.headers,
                query_params={
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "offset": offset,
                    "limit": page_size,
                },
            ) # type: ignore
            yield from resp.to_dict # type: ignore

            if not self.paginator.has_more(resp): # type: ignore
                break 

            offset = self.paginator.get_next(resp) # type: ignore

class StatsByMailboxProviderStream(SendGridStream):
    name = "stats_by_mailbox_provider"
    path = "/mailbox_providers/stats"
    primary_keys = ["date"] # type: ignore
    replication_key = "date" # type: ignore
    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("stats", th.ArrayType(
            th.ObjectType(
                th.Property('type', th.StringType),
                th.Property('name', th.StringType),
                th.Property('metrics', th.ObjectType(
                    th.Property('blocks', th.IntegerType),
                    th.Property('bounces', th.IntegerType),
                    th.Property('clicks', th.IntegerType),
                    th.Property('deferred', th.IntegerType),
                    th.Property('delivered', th.IntegerType),
                    th.Property('drops', th.IntegerType),
                    th.Property('opens', th.IntegerType),
                    th.Property('processed', th.IntegerType),
                    th.Property('requests', th.IntegerType),
                    th.Property('spam_reports', th.IntegerType),
                    th.Property('unique_clicks', th.IntegerType),
                    th.Property('unique_opens', th.IntegerType),
                ))
            )
        )),
    ).to_dict()  # type: ignore

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """

        page_size = self.page_size
        offset = 0
        start_date = self.get_starting_replication_key_value(context)
        if not start_date:
            start_date = datetime.datetime.strptime(
            self.config["start_datetime"], "%Y-%m-%dT%H:%M:%SZ"
        )
        else:
            start_date = parse(start_date)

        while True:
            resp = self.conn.client.mailbox_providers.stats.get(
                request_headers=self.headers,
                query_params={
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "offset": offset,
                    "limit": page_size,
                },
            ) # type: ignore
            yield from resp.to_dict # type: ignore

            if not self.paginator.has_more(resp): # type: ignore
                break 

            offset = self.paginator.get_next(resp) # type: ignore
