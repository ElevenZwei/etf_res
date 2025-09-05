#!/bin/bash

# load db/mdt*.csv

db_opts=(-h 124.222.94.46 -p 19005 -U option -w)
export PGPASSWORD=option

echo "loading mdt csv files from ../db/ folder."
set -e
shopt -s nullglob

function import_mdtfs {
    spot="$1"
    date="$2"
    mdfs=("../db/tick/mdt_$1"*"_$2"*".csv")
    echo "first: ${mdfs[0]}, last: ${mdfs[-1]}, count=${#mdfs[@]}"
    # mdfs=("${mdfs[@]: -1}")
    # echo "first: ${mdfs[0]}, last: ${mdfs[-1]}, count=${#mdfs[@]}"
    if command -v parallel; then
        printf "%s\n" "${mdfs[@]}" | parallel -v -j8 --halt soon,fail=1 -- \
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

# import_mdtfs 159915 20250217
# import_mdtfs 510050 20241230
# import_mdtfs 510300 20241230
# import_mdtfs 510500 20250217
# import_mdtfs 588000 20241230

# import_mdtfs 159915 20250402
# import_mdtfs 510500 20250402
# import_mdtfs 510050 20250402
# import_mdtfs 510300 20250402

# import_mdtfs 159915 20250724
# import_mdtfs 510500 20250724
# import_mdtfs 510050 20250724
# import_mdtfs 510300 20250724
# import_mdtfs 588000 20250724

import_mdtfs 159915 20250901
import_mdtfs 510500 20250901
import_mdtfs 510050 20250901
import_mdtfs 510300 20250901
import_mdtfs 588000 20250901