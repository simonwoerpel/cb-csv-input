# cb-csv-input

NOT PRODUCTION READY

simple python script to load csv data into couchbase buckets with some data manipulating &amp; formatting options

requirements: couchbase python SDK: http://docs.couchbase.com/developer/python-2.0/introduction.html

tested on python3 with couchbase-server community edition 3.0.2

## usage
    python csvtocouchbase.py data.csv [config.json]

configuration examples in config folder

if no config file specified, the script will ask for required options

