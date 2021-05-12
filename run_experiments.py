## // Run experiments //

# - for different implementations
# - for different k
# - for different number of cores
# - for different data_efficiency


import os

print("Run experiments")


scripts = ["graphframe_ex1.py"]

# scripts = ["graphframe_bs.py",
#            "graphframe_ex1.py", "graphframe_ex2.py",
#            "graphframe_ex1_bv.py", "graphframe_ex2_bv.py",
#            "rdd_bs.py", "rdd_ex.py", "rdd_ex_bv.py"]

for script, k, data_efficiency in [(script, k, data_efficiency) 
                                    for k in [1,10,100,1000]
                                    for data_efficiency in ["None", "cache", "persist"]
                                    for script in scripts
                                   ]: 
    
    script_name = "src/" + script

    print("Current values")
    print("k:",k)
    print("data_efficiency:", data_efficiency)
    print(script_name)
    
    
    spark_submit = f"spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 {script_name} {k} {data_efficiency}"
    os.system(spark_submit)
    print("-----------------END OF CURRENT RUN ------------------")


# python run_experiments.py


# Spark-submit examples
# spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 10 persist