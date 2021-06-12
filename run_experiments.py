## // Run experiments //

# --> Parameters
# 1. script: for different implementations
# 2. dataset: for different datasets (different distributions in edges)
# 3. cores: for different number of cores
# 4. k: for different k
# --> Result: Count running time


# SOS: 
# Scripts: graphframe_fast.py, graphframe_fast_bv.py, rdd_fast_bv.py, rdd_fast_bv.py require initial threshold
# this have been set in 0.8.


# Load modules
import os
from time import time
from csv import writer


print("Run experiments")

# scripts_without_init_threshold = ["graphframe_bs.py", "graphframe_fast.py", "graphframe_fast_bv.py", "rdd_bs.py", "rdd_fast.py", "rdd_fast_bv.py"]
# scripts_with_init_threshold = ["graphframe_fast.py", "graphframe_fast_bv.py", "rdd_fast.py", "rdd_fast_bv.py"]
# cores = [1,2,8]
# dataset in ["artists_uniform", "artists_normal", "artists_power_law"]
# k in [100, 1000, 10000]

for script, cores, dataset, k in [(script, cores,  dataset, k) 

                                                    for script in ["graphframe_bs.py", "rdd_bs.py"]
                                                    for cores in [8]
                                                    for dataset in ["artists_uniform"] # SOS: Change files input based on this.
                                                    for k in [2_300_000]
                                                    ]: 
    
    script_name = "src/" + script
    print("Current values:")
    print("Script:", script, "| Cores:", cores, "| Dataset:", dataset, "| k:",k)
    
    
    start = time()
    
    # Without initial threshold
    spark_submit = f"spark-submit --master local[{cores}] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 {script_name} {k}"
    
    # With initial threshold 0.8
    # spark_submit = f"spark-submit --master local[{cores}] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 {script_name} {k} 0.8"
    
    # With initial threshold 0.5
    # spark_submit = f"spark-submit --master local[{cores}] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 {script_name} {k} 0.5"
    
    
    
    os.system(spark_submit)

    end = time()
    total_time = end-start
    print("Total time:", total_time)

    with open('experiments_k_large.csv', 'a+', newline='') as experiments:  # experiments, experiments_threshold_0.5, experiments_k_large
        # Create a writer object from csv module
        csv_writer = writer(experiments)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([script, cores, dataset, k,  total_time])

    print("-----------------END OF CURRENT RUN ------------------")



## // RUN Experiments //
# $ python run_experiments.py & 


# // Spark-submit examples //

# spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_fast.py 2300000 0.5

# spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_fast_bv.py 2300000 0.5
