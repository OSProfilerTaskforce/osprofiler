# Copyright 2018 Fujitsu Ltd.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import datetime
import time
from urllib import parse as parser

from opentelemetry.trace import SpanKind
from oslo_config import cfg
from oslo_serialization import jsonutils

from osprofiler import _utils as utils
from osprofiler import exc
import logging

from osprofiler.drivers.jaeger import Jaeger
from osprofiler.drivers.utils import clean_url

LOG = logging.getLogger(__name__)


class Opentelemetry(Jaeger):
    def __init__(self, connection_str, project=None, service=None, host=None,
                 conf=cfg.CONF, **kwargs):
        """Opentelemetry driver for OSProfiler."""

        super(Opentelemetry, self).__init__(connection_str, project=project,
                                            service=service, host=host,
                                            conf=conf, **kwargs)
        self.tracer = None
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.exporter.jaeger.thrift import JaegerExporter
            self.trace = trace
            resource = Resource.create({"service.name": "{}-{}".format(project, service)})
            trace.set_tracer_provider(TracerProvider(resource=resource))
            parsed_url = parser.urlparse(connection_str)
            jaeger_exporter = JaegerExporter(
                agent_host_name=parsed_url.hostname,
                agent_port=parsed_url.port,
                udp_split_oversized_batches=True
            )
            trace.get_tracer_provider().add_span_processor(
                BatchSpanProcessor(jaeger_exporter),
            )
            self.tracer = trace.get_tracer(__name__)
        except ImportError:
            raise exc.CommandError(
                "To use OSProfiler with Uber Jaeger tracer, "
                "please install `jaeger-client` library. "
                "To install with pip:\n "
                "`pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-jaeger-thrift`."
            )
        self.spans = collections.deque()

    @classmethod
    def get_name(cls):
        return "opentelemetry"

    def notify(self, payload):
        if payload["name"].endswith("start"):
            timestamp = datetime.datetime.strptime(payload["timestamp"],
                                                   "%Y-%m-%dT%H:%M:%S.%f")
            epoch = datetime.datetime.utcfromtimestamp(0)
            start_time = (timestamp - epoch).total_seconds()
            span_context = self.trace.SpanContext(
                trace_id=utils.uuid_to_int128(payload["base_id"]),
                span_id=utils.shorten_id(payload["parent_id"]),
                is_remote=False,
                trace_flags=self.trace.TraceFlags(self.trace.TraceFlags.SAMPLED)
            )
            ctx = self.trace.set_span_in_context(self.trace.NonRecordingSpan(span_context))
            # Create Jaeger Tracing span
            span = self.tracer.start_span(
                name=operation_name(payload),
                kind=span_kind(payload),
                context=ctx,
                attributes=self.create_span_tags(payload),
                start_time=int(start_time * 1000000000)
            )
            # Replace Jaeger Tracing span_id (random id) to OSProfiler span_id
            # SpanContext is immutable and there is no easy method to set span_id at span creation
            c = self.trace.SpanContext(
                trace_id=span.context.trace_id,
                span_id=utils.shorten_id(payload["trace_id"]),
                is_remote=span.context.is_remote,
                trace_flags=span.context.trace_flags,
                trace_state=span.context.trace_state
            )
            span._context = c
            self.spans.append(span)
        else:
            span = self.spans.pop()

            # Store result of db call and function call
            for call in ("db", "function"):
                if payload.get("info", {}).get(call) is not None:
                    span.set_attribute("result", payload["info"][call]["result"])

            # Span error tag and log
            if payload["info"].get("etype") is not None:
                span.set_attribute("error", True)
                span.add_event("log", {
                    "error.kind": payload["info"]["etype"],
                    "message": payload["info"]["message"]
                })
            # Time is in nanosecond (10^9)
            span.end(end_time=int(time.time() * 1000000000))


def operation_name(payload):
    info = payload["info"]
    if info.get("request"):
        return info["request"]["method"] + "_" + clean_url(info["request"]["path"])
    if info.get("db"):
        return "SQL" + "_" + info["db"]["statement"].split(' ', 1)[0].upper()
    if info.get("requests"):
        return info["requests"]["method"]
    return removesuffix(payload["name"], "-start")


def span_kind(payload):
    span_type = {
        "wsgi": SpanKind.SERVER,
        "db": SpanKind.CLIENT,
        "http_client": SpanKind.CLIENT
    }
    return span_type.get(removesuffix(payload["name"], "-start"), SpanKind.INTERNAL)


def removesuffix(string, suffix):
    if string.endswith(suffix):
        return string[:-len(suffix)]
    return string
