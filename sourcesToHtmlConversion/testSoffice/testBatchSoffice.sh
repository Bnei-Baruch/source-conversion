#!/bin/bash


N=`date +%s%N`; export PS4='+[$(((`date +%s%N`-$N)/1000000))ms][${BASH_SOURCE}:${LINENO}]: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'; set -x;

time soffice --headless --convert-to "docx" ./Test_soffice_batch_1/*.doc --outdir ./Test_soffice_batch_results_batch/

find ./Test_soffice_batch_1/ -name "*.doc" -exec soffice --headless --convert-to "docx" --outdir ./Test_soffice_batch_results_1by1/ {} \;

date
