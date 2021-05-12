#!/bin/bash

## RUN EXPERIMENTS USING BASH

declare -a scriptsArray=("graphframe_bs.py" "graphframe_ex1.py" "graphframe_ex2.py" "graphframe_ex1_bv.py" \
                         "graphframe_ex2_bv.py" "rdd_bs.py" "rdd_ex.py" "rdd_ex_bv.py")

declare -a dataArray=("None" "cache" "persist")

declare -a kArray=(1 10 100 1000)

declare -a coresArray=(1 4 8)
 
# Read the array values with space
for script in "${scriptsArray[@]}";do
  for data_efficiency in "${dataArray[@]}"; do
    for k in "${kArray[@]}"; do
        for cores in "${coresArray[@]}"; do
            echo $script
            echo $data_efficiency
            echo $k
            echo $cores
            echo "----"

            start=`date +%s`
            spark-submit --master local[$k] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 "src/"$script $k $data_efficiency
            end=`date +%s`
            runtime=$((end-start)) 
            echo $runtime


            
        done
    done
  done
done