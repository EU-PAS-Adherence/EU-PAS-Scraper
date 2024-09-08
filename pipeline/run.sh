#!/bin/bash
echo "Running pipeline..."
scrapy ema_rwd -F 100 -o $(dirname "$0")/data.xlsx --logfile "$(dirname "$0")/ema_rwd_$(date -u +'%Y-%m-%dT%H-%M-%S').log"
scrapy patch -i $(dirname "$0")/data.xlsx -o $(dirname "$0") -mi $(dirname "$0")/sponsors_manual.xlsx -mc -ac match state cancel --logfile "$(dirname "$0")/patch_$(date -u +'%Y-%m-%dT%H-%M-%S').log"
scrapy ema_rwd_statistic -i $(dirname "$0")/data_patched.xlsx -o $(dirname "$0") -D $COMPARE_DATE  --logfile "$(dirname "$0")/statistic_$(date -u +'%Y-%m-%dT%H-%M-%S').log"
echo "Finished pipeline."
