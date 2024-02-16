#!/bin/bash
echo "Running pipeline..."
scrapy eupas -F 100 -o $(dirname "$0")/data.xlsx --logfile "$(dirname "$0")/eupas_$(date -u +'%Y-%m-%dT%H-%M-%S').log"
scrapy patch -i $(dirname "$0")/data.xlsx -o $(dirname "$0") -ci $(dirname "$0")/centre_manual.xlsx -cc -dcf centre_match cancel --logfile "$(dirname "$0")/patch_$(date -u +'%Y-%m-%dT%H-%M-%S').log"
scrapy statistic -i $(dirname "$0")/data_patched.xlsx -o $(dirname "$0") --logfile "$(dirname "$0")/statistic_$(date -u +'%Y-%m-%dT%H-%M-%S').log"
echo "Finished pipeline."
