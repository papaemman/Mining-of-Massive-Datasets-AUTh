# Course: Mining-of-Massive-Datasets-AUTh

## MSc Data and Web Science 2020-2021

![MSC-LOGO](./assets/dws-logo.png)

## Team 8 - Members

* Panagiotis Papaemmanouil, 56
* Dimitris Tourgaidis, 66

---

## Assignment: “Top-k Most Probable Triangles in Uncertain Graphs”

In this project, you will work with graph data. Given a potentially large probabilistic network and an integer
k, you must design and implement a solution to discover the top-k most probable triangles. A probabilistic
network is a network where edges are annotated with existential probabilities. This means that an edge e is
present with a probability p(e). Therefore, if a triangle is formed among the edges a, b and c, then the
existential probability of the triangle is `p(a)*p(b)*p(c)`

[Assignment](./docs/mmd-project1-2021.pdf)

[Report](./docs/Report_MiningOfMassiveDataset.pdf)

---

## Technology Stack

![PySpark](./assets/PySpark-1024x164.png)

1. Python: <https://www.python.org/>
2. Spark: <https://spark.apache.org/docs/latest/index.html>
3. PySpark: <https://spark.apache.org/docs/latest/api/python/index.html>
4. Graphframes spark package: <https://graphframes.github.io/graphframes/docs/_site/quick-start.html>

---

## Project Setup

\
**Create a docker container based on ubuntu as base image and install java, javac, spark, python**

```bash
# Create docker container based on ubuntu base image and get a console inside it
sudo docker run --it --name ApacheSpark -p 1234:1234 ubuntu bin/bash  
```

Download and unzip spark (version: spark-3.1.1-bin-hadoop2.7.tgz) on host machine from <https://spark.apache.org/downloads.html>

```bash
# Copy spark from host machine to container
sudo docker cp ./path_to_spark/spark-3.1.1-bin-hadoop2.7 ApacheSpark:.  
```

Setup container. Run the following command from inside the container

```bash
# Update container packages
apt update

# Install java
apt install default-jre 

# Install jdk compiler
$ apt install default-jdk  

# Setup environmental variables
apt-get install nano
sudo nano /etc/environment 

# Add these lines to `/etc/environment` file
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64" 
export PATH=$PATH:spark-3.1.1-bin-hadoop2.7/bin
export PATH=$PATH:spark-3.1.1-bin-hadoop2.7/sbin
export PYSPARK_PYTHON=/usr/bin/python3.8
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.8
export SPARK_HOME=spark-3.1.1-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python

# Save and Close file

# Source changes
$ source /etc/environment 

# Install python
apt-get install python3.8
apt-get install python3.8-venv
apt-get install python3-pip

# Install numpy
$ python3.8 -m pip install numpy
```

\
**Check installations**

```bash
# Check versions
java --version
java --version

# Check python
python3.8

# Check pyspark and spark-shell interactive terminals
pyspark
spark-shell
```

\
**Helpful Docker commands**

```bash
docker ps
docker ps -a
docker start ApacheSpark
docker stop ApacheSpark
docker rm ApacheSpark
```

---

\
**How to run src/ scripts using spark-submit**

```bash
spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 2 cache
spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_bs.py 2
```

---

## Spark Notes

### Debugging spark packages

Spark-submit takes the flag --packages and search for packages.
These packages can be found localy or can be downloaded via internet connection. For the specific case of `graphframes` spark package, the download process isn't straight-forward, because only the source code was available online and not the .jar file.

* Check here for the source code: <https://spark-packages.org/package/graphframes/graphframes>
* Check in the `jars/graphframes_graphframes-0.8.1-spark3.0-s_2.12.jar` directory for the graphframes.jar file.

**The solution was to manualy build the .jar file and copy it to one of the desired paths**. For example, we need to copy the .jar file to one of the following paths.

```bash
# Copy the graphframes.jar file to one of the following paths
/home/user7/.ivy2/local/graphframes/graphframes/0.8.1-spark3.0-s_2.12/jars/graphframes.jar

/home/user7/.m2/repository/graphframes/graphframes/0.8.1-spark3.0-s_2.12/graphframes-0.8.1-spark3.0-s_2.12.jar
```

### How to disable spark INFO messages

In order to suppress warning during Apache spark execution, you need to modify the `conf/log4j/properties` file.

```bash
cp log4j.properties.template log4j.properties
nano log4j.properties

# and change the following line
log4j.rootCategory=INFO, console --> log4j.rootCategory=ERROR, console
```

---

## Dataset information

SNAP Artists dataset: <https://snap.stanford.edu/data/gemsec-Facebook.html>

 We evaluate our implementations through a Real World dataset provided from the Stanford Network Analysis Project repository (SNAP\footnote{\href{https://snap.stanford.edu/index.html}{Snap website}}), which is the artists dataset\footnote{\href{https://snap.stanford.edu/data/gemsec-Facebook.html}{Artist dataset link}}. This dataset referred to Facebook pages and it was collected in November 2017. It represents blue verified Facebook page networks. The network's nodes represent the artists' pages and the network's edges represent the mutual likes among them.

* Nodes: 50,515
* Edges: 819,306
* Total triangles: 2,273,700
