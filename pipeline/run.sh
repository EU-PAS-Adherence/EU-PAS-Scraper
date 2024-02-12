#!/bin/bash
echo "Running pipeline..."
echo "$(dirname "$0")/output.xlsx"
scrapy eupas -F rmp2 -o $(dirname "$0")/data.xlsx
scrapy patch -i $(dirname "$0")/data.xlsx -o $(dirname "$0") -ci $(dirname "$0")/centre_manual.xlsx -cc -dcf centre_match cancel
scrapy statistic -i $(dirname "$0")/data_patched.xlsx -o $(dirname "$0")
echo "Finished pipeline."
