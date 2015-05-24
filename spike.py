from __future__ import division
from config import public_dir
import os
import zipfile
import zlib
import json
from pprint import pprint
import collections


def main():
    base_fp = "24-05-2015_11"
    input_fp = base_fp + ".zip"
    input_tps_fp = base_fp + "/tps.json.gz"

    input_f = zipfile.ZipFile(input_fp, "r")
    compressed_tps = input_f.read(input_tps_fp)
    tps_f = zlib.decompress(compressed_tps, 16 + zlib.MAX_WBITS)
    tps_dict = json.loads(tps_f)
    tps_dict = {int(float(k)):v for k,v in tps_dict.items()}

    ema = {}
    growth = {}
    ema_length = 20
    growth_length = 5
    k = 2 / (ema_length + 1)
    prev_ema = 0
    for i, (timestamp, tps) in enumerate(tps_dict.iteritems()):
        # Calculates EMA
        if i == ema_length:
            avg = sum(tps_dict.values()[:ema_length]) / ema_length
            ema[timestamp] = avg
            prev_ema = ema[timestamp]
        elif i > ema_length:
            ema[timestamp] = calculate_ema(tps, prev_ema, k)
            prev_ema = ema[timestamp]

        # Calculates EMA growth
        if i >= ema_length + growth_length:
            growth[timestamp] = (ema[timestamp] - ema[timestamp - growth_length]) / ema[timestamp - growth_length]

    ordered_ema = collections.OrderedDict(sorted(ema.items()))
    ema_graph_fp = public_dir + 'ema.json'
    with open(ema_graph_fp, 'w') as f:
        f.write(json.dumps(ordered_ema))

    ordered_growth = collections.OrderedDict(sorted(growth.items()))
    growth_graph_fp = public_dir + 'growth.json'
    with open(growth_graph_fp, 'w') as f:
        f.write(json.dumps(ordered_growth))


def calculate_ema(current_value, prev_ema, k):
    return current_value * k + prev_ema * (1 - k)


if __name__ == '__main__':
    main()