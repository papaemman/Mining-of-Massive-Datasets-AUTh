## // Run experiments //

# 1. script: for different implementations
# 2. dataset: for different datasets (different distributions in edges)
# 3. cores: for different number of cores
# 4. k: for different k
# 5. data_efficiency: for different data_efficiency methods (cache, persistance)
# Result: Cound running time


import os
from time import time
from csv import writer


print("Run experiments")

# ["graphframe_bs.py", "graphframe_ex1_bv.py", "graphframe_ex1.py",
#  "graphframe_ex2_bv.py",  "graphframe_ex2.py",
#  "rdd_bs.py", "rdd_ex_bv.py", "rdd_ex.py", "rdd_fast.py"]

# for data_efficiency in ["None", "cache", "persist"]

# for dataset in ["normal", "uniform", "power_law"]

for script, cores, dataset, k in [(script, cores,  dataset, k) 

                                                    for script in ["rdd_bs.py", "rdd_fast.py"] # "graphframe_bs.py", 
                                                    for cores in [1,2,8]
                                                    for dataset in ["artists_uniform"] # "artists_uniform", "artists_normal", "artists_power_law"
                                                    for k in [10, 100, 1000]
                                                    
                                                    ]: 
    
    script_name = "src/" + script
    print("Current values:")
    print("Script:", script_name, "| Cores:", cores, "| Dataset:", dataset, "| k:",k)
    
    
    start = time()
    
    spark_submit = f"spark-submit --master local[{cores}] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 {script_name} {k}"
    os.system(spark_submit)

    end = time()
    total_time = end-start
    print("Total time:", total_time)

    with open('experiments.csv', 'a+', newline='') as experiments:
        # Create a writer object from csv module
        csv_writer = writer(experiments)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([script_name, cores, dataset, k,  total_time])

    print("-----------------END OF CURRENT RUN ------------------")



## // RUN Experiments //
# $ python run_experiments.py

# // Spark-submit examples //
# spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_fast.py 1000 0.8