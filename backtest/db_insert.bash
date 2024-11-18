#!/bin/bash

# load db/ci*.csv and db/md*.csv

db_opts=(-h localhost -p 15432 -U option)
export PGPASSWORD=option

echo "loading ci csv files from ./db/ folder."
set -e
shopt -s nullglob

# import cifs
function import_cifs {
    cifs=("./db/ci_159915"*"_202"[4]*".csv")
    echo "${cifs[@]}"
    for cif in "${cifs[@]}"; do
        psql "${db_opts[@]}" -d opt -c "\copy contract_info from '${cif}' delimiter ',' csv header";
    done;
}

function import_mdfs {
    mdfs=("./db/md_159915"*"_202"[4]*".csv")
    echo "${mdfs[@]}"
    for mdf in "${mdfs[@]}"; do
        echo "importing $mdf"
        psql "${db_opts[@]}" -d opt -c "\copy market_data from '${mdf}' delimiter ',' csv header";
    done;
}

# import_cifs
import_mdfs
