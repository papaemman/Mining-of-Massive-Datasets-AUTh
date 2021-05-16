# TASKS

[X] EDA for collins.txt graph dataset (Plots, Descriptive statistics, Distributions)

[X] Implement a baseline solution using `graphframes`

[X] Implement a low level solution

[X] Run experiments for different implementations (3+1), different number of k and different number of cores, nocach()/cache()/persist() => time

[X] Sanity check. Compare results (not time)

1. graphframe_bs.py

```bash
spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 2 cache

    Row(Triangle='129,131,137', Triangle_Prob=0.9702989999999999)
    Row(Triangle='1127,1128,1129', Triangle_Prob=0.9702989999999999)
    Execution time : 24.123802185058594


    Row(Triangle='130,133,143', Triangle_Prob=0.9702989999999999)
    Row(Triangle='347,352,353', Triangle_Prob=0.9702989999999999)
    Execution time : 20.97762179374695

spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 2 cache

    Row(Triangle='872,881,882', Triangle_Prob=0.9702989999999999)
    Row(Triangle='854,929,935', Triangle_Prob=0.9702989999999999)
    Row(Triangle='72,76,80', Triangle_Prob=0.9702989999999999)
    Row(Triangle='535,538,550', Triangle_Prob=0.9702989999999999)
    Row(Triangle='735,736,737', Triangle_Prob=0.9702989999999999)
    Row(Triangle='441,445,453', Triangle_Prob=0.9702989999999999)
    Row(Triangle='741,743,746', Triangle_Prob=0.9702989999999999)
    Row(Triangle='35,37,40', Triangle_Prob=0.9702989999999999)
    Row(Triangle='846,849,937', Triangle_Prob=0.9702989999999999)
    Row(Triangle='347,352,353', Triangle_Prob=0.9702989999999999)
Execution time : 27.456836223602295
```

Different results triangles on each experiment, but with the same probability!

2. rdd_bs.py

```bash
spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_bs.py 10

    ('1,3,4', 0.9702989999999999)
    ('1,2,4', 0.9702989999999999)
    ('10,8,9', 0.9702989999999999)
    ('10,7,8', 0.9702989999999999)
    ('10,7,9', 0.9702989999999999)
    ('24,918,922', 0.9702989999999999)
    ('24,917,918', 0.9702989999999999)
    ('24,917,922', 0.9702989999999999)
    ('24,915,922', 0.9702989999999999)
    ('24,915,917', 0.9702989999999999)
    Execution time : 8.00922679901123
```

3. rdd_fast.py

```bash
spark-submit --master local[3] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_fast.py 10 0.8

    ('128,141,143', 0.9702989999999999)
    ('128,140,143', 0.9702989999999999)
    ('128,138,141', 0.9702989999999999)
    ('128,138,143', 0.9702989999999999)
    ('128,137,141', 0.9702989999999999)
    ('128,137,143', 0.9702989999999999)
    ('128,137,138', 0.9702989999999999)
    ('128,136,138', 0.9702989999999999)
    ('128,136,137', 0.9702989999999999)
    ('128,136,141', 0.9702989999999999)
    Execution time : 7.485780954360962

```


[ ] Create synthetic uncertain graphs with different size and edges probablity distributions.

[X] Write report (overleaf)

Overleaf: <https://www.overleaf.com/project/6092d7d4dc238d5f619dc701>

[X] Presentation (google slides)

DONE!
