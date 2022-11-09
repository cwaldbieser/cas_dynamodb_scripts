
CAS DynamoDB Scripts
====================

Helper scripts for working with the DynamoDB ticket backend for Apereo CAS.

Usage
-----

get_expired_objects.py
""""""""""""""""""""""

    usage: Get expired object IDs. [-h] [--timestamp-name TIMESTAMP_NAME] [--offset OFFSET] [-o OUTFILE] table

    positional arguments:
      table                 The table to operate on.

    optional arguments:
      -h, --help            show this help message and exit
      --timestamp-name TIMESTAMP_NAME
                            The name of the time stamp attribute to filter against.
      --offset OFFSET       The number of seconds in the past to start filtering back.
      -o OUTFILE, --outfile OUTFILE
                            CSV output file.

delete_objects.py
"""""""""""""""""

    usage: Delete DynamoDB objects. [-h] [--id-attrib ID_ATTRIB] [--column COLUMN] table infile

    positional arguments:
      table                 The table to operate on.
      infile                CSV input file.

    optional arguments:
      -h, --help            show this help message and exit
      --id-attrib ID_ATTRIB
                            The name of the object ID attribute (in the table).
      --column COLUMN       The name of the object ID column (in the CSV file).

Example
-------

.. code::shell

   $ ./get_expired_objects.py cas-tgt-table --offset 28800 -o /tmp/expired-tgts.csv
   $ ./delete_objects.py cas-tgt-table /tmp/expired-tgts.csv

