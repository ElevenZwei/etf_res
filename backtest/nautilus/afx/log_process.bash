#!/bin/bash

# log file parse net worth line
fname="$1"
output_dir="../../data/output/"
iconv -f UTF-16 -t UTF-8 "../../log/$fname" > "$output_dir/$fname"

grep 'net_worth'  "$output_dir/$fname" | cut -d' ' -f1,4 |\
        sed 's/net_worth=//;s/,//;' | sed -r 's/\x1B\[[0-9;]*[a-zA-Z]//g' |\
        tr ' ' ',' > "$output_dir/${fname%.*}_net_worth.csv"

grep 'impv'  "$output_dir/$fname" | cut -d' ' -f1,4,5 |\
        sed 's/atm_impv=//;s/impv_ratio=//;' | sed -r 's/\x1B\[[0-9;]*[a-zA-Z]//g' |\
        tr ' ' ',' > "$output_dir/${fname%.*}_impv.csv"

python log_net_worth_ratio.py --prefix "${fname%.*}"
