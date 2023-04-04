#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import csv
from datetime import datetime
import collections

from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key="", sep="__"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def persist_messages(messages, output_folder):
    delimiter = ","
    quotechar = '"'

    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    date = datetime.now().strftime("%Y-%m-%d")

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o["type"]
        if message_type == "RECORD":
            if o["stream"] not in schemas:
                raise Exception(
                    "A record for stream {}"
                    "was encountered before a corresponding schema".format(o["stream"])
                )

            validators[o["stream"]].validate(o["record"])

            filename = o["stream"] + f"--{date}.csv"
            filename = os.path.join(output_folder, filename)
            file_is_empty = (not os.path.isfile(filename)) or os.stat(
                filename
            ).st_size == 0

            flattened_record = flatten(o["record"])

            # get headers from file if not empty and we haven't stored them yet
            if o["stream"] not in headers and not file_is_empty:
                with open(filename, "r") as csvfile:
                    reader = csv.reader(
                        csvfile, delimiter=delimiter, quotechar=quotechar
                    )
                    first_line = next(reader)
                    headers[o["stream"]] = (
                        first_line if first_line else flattened_record.keys()
                    )
            else:
                headers[o["stream"]] = flattened_record.keys()

            with open(filename, "a") as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    headers[o["stream"]],
                    extrasaction="ignore",
                    delimiter=delimiter,
                    quotechar=quotechar,
                )
                if file_is_empty:
                    writer.writeheader()

                writer.writerow(flattened_record)

            state = None
        elif message_type == "STATE":
            logger.debug("Setting state to {}".format(o["value"]))
            state = o["value"]
        elif message_type == "SCHEMA":
            stream = o["stream"]
            schemas[stream] = o["schema"]
            validators[stream] = Draft4Validator(o["schema"])
            key_properties[stream] = o["key_properties"]
        else:
            logger.warning("Unknown message type {} in message {}".format(o["type"], o))

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = persist_messages(
        input_messages,
        config["output_folder"],
    )

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()
