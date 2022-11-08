#! /usr/bin/env python

import argparse
import csv
import datetime
import sys

import boto3
from boto3.dynamodb.conditions import Attr


def main(args):
    """
    Main entrypoint.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)
    print("Table:", args.table)
    print("Timestamp name:", args.timestamp_name)
    print("Offset:", args.offset)
    timestamp = get_timestamp(args.offset)
    print("Timestamp:", timestamp)
    writer = csv.writer(args.outfile)
    writer.writerow(["obj_id", "creation_time"])
    for item in scan(table, args.timestamp_name, timestamp):
        obj_id = item["id"]
        create_time = item["creationTime"]
        writer.writerow((obj_id, create_time))


def scan(table, timestamp_name, timestamp):
    """
    A paged scan.
    """
    kwds = {}
    while True:
        response = table.scan(
            FilterExpression=Attr(timestamp_name).lt(timestamp), **kwds
        )
        # LastEvaluatedKey
        print("Count:", response["Count"])
        for item in response["Items"]:
            yield item
        last_evaluated_key = response.get("LastEvaluatedKey")
        if last_evaluated_key is None:
            break
        kwds["ExclusiveStartKey"] = last_evaluated_key


def get_timestamp(offset):
    return "{}Z".format(
        (datetime.datetime.utcnow() - datetime.timedelta(seconds=30)).isoformat()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Perform DynamoDB bulk operations.")
    parser.add_argument("table", action="store", help="The table to operate on.")
    parser.add_argument(
        "--timestamp-name",
        default="creationTime",
        action="store",
        help="The name of the time stamp attribute to filter against.",
    )
    parser.add_argument(
        "--offset",
        default=30,
        action="store",
        type=int,
        help="The number of seconds in the past to start filtering back.",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        default=sys.stdout,
        type=argparse.FileType("w"),
        help="CSV output file.")
    args = parser.parse_args()
    main(args)
