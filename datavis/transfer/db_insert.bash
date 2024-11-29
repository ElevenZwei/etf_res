#!/bin/bash

# load db/mdt*.csv

db_opts=(-h localhost -p 15432 -U option)
export PGPASSWORD=option

echo "loading mdt csv files from ../db/ folder."
set -e
shopt -s nullglob

function import_mdtfs {
    spot="$1"
    date="$2"
    mdfs=("../db/mdt_$1"*"_$2"*".csv")
    echo "first: ${mdfs[0]}, last: ${mdfs[-1]}, count=${#mdfs[@]}"
    # mdfs=("${mdfs[@]: -1}")
    # echo "first: ${mdfs[0]}, last: ${mdfs[-1]}, count=${#mdfs[@]}"
    if command -v parallel; then
        find . -type f -wholename './db/mdt_*.csv' | parallel -v -j8 --halt soon,fail=1 -- \
            psql "${db_opts[@]}" -d opt -c "\"\copy market_data_tick from '{}' delimiter ',' csv header\"";
    else
        for mdf in "${mdfs[@]}"; do
            echo "importing $mdf"
            psql "${db_opts[@]}" -d opt -c "\copy market_data_tick from '${mdf}' delimiter ',' csv header";
        done;
    fi
}


# import_mdtfs 510050 20241101
# import_mdtfs 510050 20241105
# import_mdtfs 510050 20241106

# import_mdtfs 159915 20241118
# import_mdtfs 510050 20241118
# import_mdtfs 510300 20241118
# import_mdtfs 510500 20241118
# import_mdtfs 588000 20241118

import_mdtfs 159915 20241125
import_mdtfs 510050 20241125
import_mdtfs 510300 20241125
import_mdtfs 510500 20241125
import_mdtfs 588000 20241125
