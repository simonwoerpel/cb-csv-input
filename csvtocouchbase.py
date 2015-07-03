import os
import codecs
import argparse
import csv
import simplejson as json
import uuid
from datetime import datetime
from couchbase.bucket import Bucket


def ask_to_continue(prompt):
    while True:
        if not str(input(prompt+'\npress [y] to continue or ctrl-c to abort\n')) == 'y':
            continue
        else:
            break
    return True


def ask_for_bool(prompt):
    while True:
        bool_input = str(input(prompt+' [y/n]\n'))
        if not bool_input in ('y','n'):
            continue
        else:
            break
    if bool_input == 'y':
        return True
    else:
        return False


def reassign_keys(header):
    # TODO not working yet
    while True:
        if str(input('are these keys ok? [y/n]\n')) == 'y':
            new_keys = header.split(';')
            break
        else:
            while True:
                new_keys = input('specify comma seperated list of new keys:\n')
                if not len(new_keys.split(',')) == len(header.split(';')):
                    print('number of new keys is not matching number of old keys')
                    continue
                else:
                    print('new keys OK')
                    break
    return new_keys


def couchbase_connect(config):
    if not 'couchbase_connect' in config:
        host = str(input('host: '))
        bucket = str(input('bucket name: '))
        pwd = str(input('password: '))
    else:
        bucket = config['couchbase_connect']['bucket']
        pwd = config['couchbase_connect']['pwd']
        if 'host' in config['couchbase_connect']:
            host = config['couchbase_connect']['host']
        else:
            # default
            host = 'localhost'
    try:
        c = Bucket('couchbase://%s/%s' % (host, bucket), password=pwd)
    except Exception as e:
        print(e)
        print('cannot connect to couchbase')
    return c


def get_id_field(sample):
    while True:
        id_field = str(input('what is the id field? '))
        if not id_field in list(sample.keys()):
            print('%s is not in %s' % (id_field, list(sample.keys())))
            continue
        else:
            break
    return id_field


def convert_record(r, date_fields=[], decimal_fields=[], replacing={}, extra_data=None):
    """
    converts r (already a dict from csv.DictReader) into better dict for couchbase
    currently only date formatting, decimal formatting, replacing of values
    """
    d = {}
    clean_d = { k:v.strip() for k, v in r.items()}
    for k in clean_d.keys():
        if clean_d[k] and k in date_fields:
            try:
                d[k] = datetime.strptime(clean_d[k], date_strformat).date().isoformat()
            except ValueError as e:
                raise(e)
                d[k] = clean_d[k]
        elif k in decimal_fields:
            try:
                d[k] = float(clean_d[k])
            except ValueError:
                try:
                    # assuming numbers have , as decimal delimiter and . as thousands seperator!
                    # FIXME
                    d[k] = float(clean_d[k].replace('.','').replace(',', '.'))
                except ValueError:
                    d[k] = clean_d[k]
        elif k in [f for f in replacing]:
            try:
                d[k] = replacing[k][clean_d[k]]
            except KeyError:
                d[k] = clean_d[k]
        else:
            d[k] = clean_d[k]
    if extra_data:
        for k in extra_data.keys():
            if k not in list(d.keys()):
                d[k] = extra_data[k]
    return d


# ARGS

parser = argparse.ArgumentParser()
parser.add_argument('inputfile', type=str, help='the input file, must be .csv with ONLY 1 header row and delimited by ;')
parser.add_argument('configfile', type=str, help='the config file, must be .json dict', nargs='?', default=None)
args = parser.parse_args()
input_fp = args.inputfile
config = json.load(open(args.configfile))


# BEGIN


print('processing '+input_fp)
if config:
    print('with config for %s' % ', '.join([k for k in config]))

if not os.path.isfile(input_fp):
    raise Exception(input_fp+' cannot be found')


print('fetching keys...')
try:
    with open(input_fp) as f:
        header = f.readline()
except UnicodeDecodeError:
    with codecs.open(input_fp, 'r', 'iso-8859-1') as f:
        header = f.readline()

print('these are the keys:\n')
print('\n'.join(header.split(';')))
ask_to_continue('are these keys ok?')        
# new_keys = reassign_keys(header)
# print('...using these keys:\n\n'+'\n'.join(new_keys))


print('loading file records into memory...')


try:
    with open(input_fp) as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = []
        i = 0
        for row in reader:
            rows.append(row)
            i += 1
            if i == 1000:
                print('.', end='')
                i = 0
except UnicodeDecodeError:
    with codecs.open(input_fp, 'r', 'iso-8859-1') as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = []
        i = 0
        for row in reader:
            rows.append(row)
            i += 1
            if i == 1000:
                print('.', end='')
                i = 0


print('\nOK. '+input_fp+' has '+str(len(rows))+' records\n')
print('this would be a sample json record:\n')
print(json.dumps(convert_record(rows[0])))


# DATE FORMATTING

if not 'date_formatting' in config:
    change = ask_for_bool('want to change date formatting?')

    if change:
        print('changing date formatting')
        while True:
            date_fields = [f.strip() for f in input('enter comma seperated list of field keys that should converted into isoformat:\n').split(',')]
            if not set(date_fields) < set(list(rows[0].keys())):
                print('these keys are not in %s' % ', '.join(list(rows[0].keys())))
                continue
            else:
                date_strformat = str(input('enter python date input format (e.g. %d.%m.%Y)'))
                break
        print('generating new sample record...')
        d = convert_record(rows[0], date_fields)
        print('...this is how it looks like now:')
        print(json.dumps(d))
    else:
        date_fields = []
        print('ok, nothing to change.')
else:
    date_fields = config['date_formatting']['fields']
    date_strformat = config['date_formatting']['strformat']
    print('changing date formating (input: %s) for fields %s...' % (date_strformat, date_fields))
    print('generating new sample record...')
    d = convert_record(rows[0], date_fields)
    print('...this is how it looks like now:')
    print(json.dumps(d))


# DECIMAL

if not 'decimal_fields' in config:
    change = ask_for_bool('want to change decimal formatting?')

    if change:
        print('changing decimal formatting')
        while True:
            decimal_fields = [f.strip() for f in input('enter comma seperated list of field keys that should converted into decimal:\n').split(',')]
            if not set(decimal_fields) < set(list(rows[0].keys())):
                print('these keys are not in %s' % ', '.join(list(rows[0].keys())))
                continue
            else:
                break
        print('generating new sample record...')
        d = convert_record(rows[0], date_fields, decimal_fields)
        print('...this is how it looks like now:')
        print(json.dumps(d))
    else:
        decimal_fields = []
        print('ok, nothing to change.')
else:
    decimal_fields = config['decimal_fields']
    print('changing decimal formating for fields %s...' % decimal_fields)
    print('generating new sample record...')
    d = convert_record(rows[0], date_fields, decimal_fields)
    print('...this is how it looks like now:')
    print(json.dumps(d))


# REPLACING

if 'replacing' in config:
    print('replacing with this scheme:')
    replacing = config['replacing']
    print('\n%s' % replacing)
    d = convert_record(rows[0], date_fields, decimal_fields, replacing)
    print('...this is how it looks like now:')
    print(json.dumps(d))
    ask_to_continue('Cool?')
else:
    replacing = {}


# EXTRA DATA

if not 'extra_data' in config:
    add_data = ask_for_bool('do you want to add some data to each record?')

    if add_data:
        while True:
            append_dict = str(input('insert extra data as valid json dict:\n'))
            try:
                extra_data = json.loads(append_dict)
                break
            except Exception as e:
                print(e)
                continue
    else:
        extra_data = {}
else:
    print('using extra data from config...')
    extra_data = config['extra_data']
    print(extra_data)


print('generating json sample with extra data')
print(json.dumps(convert_record(rows[0], date_fields, decimal_fields, replacing, extra_data)))

ask_to_continue('looks good?')

# id_field = get_id_field(rows[0])

# print('using %s as id field... OK' % id_field)


if not 'upsert_package' in config:
    upsert_package = int(input('how many records should be upserted at once?\n'))
else:
    upsert_package = config['upsert_package']
    print('using upsert_package of %s from config' % upsert_package)

steps = int(len(rows)//upsert_package+1)


print('breaking %s total records in %s upsert steps with %s each...' % (
    len(rows),
    steps,
    upsert_package)
    )


# print('performing some tests for first upsert_package...')
print('performing FULL TEST...')
upsert = {}
for i in range(0, steps):
    # FULL TEST
    upsert[i] = {}
    for row in rows[(i*upsert_package):((i+1)*upsert_package)]:
        try:
            upsert[i][str(uuid.uuid4())] = convert_record(row, date_fields, decimal_fields, replacing, extra_data)
        except Exception as e:
            print('ERROR:\n')
            print(e)
            print('error occured in bucket %s' % i)
            print(row)
            raise


print('test about %s records successfull!' % len(upsert))


print('\nthis kind of data will be imported:\n')
print(list(upsert.items())[0])


ask_to_continue('start importing?')


# COUCHBASE

print('connecting to couchbase...')

c = couchbase_connect(config)

print(c)
print('...OK')


# START IMPORT

print('importing '+str(len(rows))+' records into couchbase...')


for i in range(0, steps):
    # upsert = {}
    # for row in rows[(i*upsert_package):((i+1)*upsert_package)]:
    #     upsert[str(uuid.uuid4())] = convert_record(row, date_fields, decimal_fields, replacing, extra_data)
    # we have that already because of FULL TEST (see above)
    try:
        c.upsert_multi(upsert[i])
    except Exception as e:
        print('error while couchbase upsert:\n')
        print(e)
    print('upserted %s of %s...' % (((i+1)*upsert_package), len(rows)))

print('END: successfully imported %s records into couchbase!' % len(rows))

