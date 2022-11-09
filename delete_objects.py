#! /usr/bin/env python

import argparse
import csv
import datetime
import itertools

import boto3


def main(args):
    """
    Main entrypoint.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)
    column = args.column
    id_attrib = args.id_attrib
    print("Table:", args.table)
    print("ID attribute:", id_attrib)
    print("Object ID column:", column)
    reader = csv.DictReader(args.infile)
    with table.batch_writer() as batch:
        for item in iterprogress(reader):
            obj_id = item[column]
            batch.delete_item(Key={id_attrib: obj_id})
            print("Deleted object ID: {}".format(obj_id))


def iterprogress(it, batch_size=500, msg=None):
    """
    Show progress while iterating.
    """
    if msg is None:
        msg = "Processing the next {} items ...".format(batch_size)
    cycle = itertools.cycle(range(batch_size))
    for n, item in zip(cycle, it):
        if n == 0:
            dt = datetime.datetime.today()
            dtstr = dt.isoformat()
            print("[{}] {}".format(dtstr, msg))
        yield item


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Delete DynamoDB objects.")
    parser.add_argument("table", action="store", help="The table to operate on.")
    parser.add_argument(
        "--id-attrib",
        default="id",
        action="store",
        help="The name of the object ID attribute (inthe table).",
    )
    parser.add_argument(
        "--column",
        default="obj_id",
        action="store",
        help="The name of the object ID column (in the CSV file).",
    )
    parser.add_argument("infile", type=argparse.FileType("r"), help="CSV input file.")
    args = parser.parse_args()
    main(args)
