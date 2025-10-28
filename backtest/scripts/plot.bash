#!/bin/bash


while getopts "f:h" arg; do
    case $arg in
        f)
            input_file="$OPTARG"
            ;;
        h)
            echo "Usage: $0 [-f input_file]"
            exit 0
            ;;
        *)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
    esac
done

plot_cmd=$(cat << END
    set terminal png size 1366,768;
    set output 'backtest/data/plot/$(basename "$input_file").png';
    set title 'PNL';
    set xlabel 'Date';
    set ylabel 'PNL';
    set grid;
    set xdata time;
    set timefmt '%Y-%m-%d';
    set format x '%Y-%m-%d';
    set datafile separator ',';
    plot '$input_file' using 1:2 with lines title 'short' lw 2 lc rgb 'blue';
END
)

gnuplot -e "$plot_cmd"
