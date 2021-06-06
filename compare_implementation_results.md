# Compare implementation results

Files: `graphframe_bs.py`, `graphframe_fast.py`, `rdd_bs.py`, `rdd_fast.py`

## All algorithms with ex_exploit_bug.csv dataset

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 10
```

All triangles: 3
Row(Triangle='1,2,3', Triangle_Prob=0.69)
Row(Triangle='4,5,6', Triangle_Prob=0.5112)
Row(Triangle='2,4,5', Triangle_Prob=0.357911)

Execution time : 27.514233589172363


1. `graphframe_bs.py`

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 2
```

**Correct**
Row(Triangle='1,2,3', Triangle_Prob=0.69)
Row(Triangle='4,5,6', Triangle_Prob=0.5112)
Execution time : 24.46819567680359

2. `graphframe_ex1.py`

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_ex1.py 2
```

**Error**
Row(Triangle='4,5,6', Triangle_Prob=0.5112)
Row(Triangle='2,4,5', Triangle_Prob=0.357911)
Execution time : 18.49388360977173

3. `graphframe_ex2.py`

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_ex2.py 2
```

**Error**
Row(Triangle='4,5,6', Triangle_Prob=0.5112)
Row(Triangle='2,4,5', Triangle_Prob=0.357911)
Execution time : 18.49388360977173

3. `graphframe_fast.py`

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_fast.py 2 0.7
```

**Correct**
Row(Triangle='1,2,3', Triangle_Prob=0.69)
Row(Triangle='4,5,6', Triangle_Prob=0.5112)
Execution time : 15.841185808181763

4. `rdd_bs.py`

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_bs.py 2
```

**Correct**
('1,2,3', 0.69)
('4,5,6', 0.5112)
Execution time : 3.980290412902832

5. `rdd_ex.py`

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_ex.py 2
```

**Error**
('4,5,6', 0.5112)
('2,4,5', 0.357911)
Execution time : 4.0134899616241455

5. `rdd_fast.py`

```bash
spark-submit --master local[1] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_fast.py 2 0.7
```

**Correct**
('1,2,3', 0.69)
('4,5,6', 0.5112)
Execution time : 4.892612457275391

### Conclusions

Implementations: `graphframe_bs.py`, `graphframe_fast.py`, `rdd_bs.py`, `rdd_fast.py`, gives the correct results,
while  `graphframe_ex1.py`, `graphframe_ex2.py`, `rdd_ex.py` gives incorrect results.

---

## Correct files with large dataset (artists_uniform.csv)

1. `graphframe_bs.py`

```bash
spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 10
```

Row(Triangle='13512,16091,20525', Triangle_Prob=0.9845106014259455)
Row(Triangle='32822,44147,46375', Triangle_Prob=0.9813710013541554)
Row(Triangle='1335,5686,18634', Triangle_Prob=0.9812171022432823)
Row(Triangle='38879,41049,43941', Triangle_Prob=0.9811032283661145)
Row(Triangle='1758,9788,40666', Triangle_Prob=0.9803237550574053)
Row(Triangle='167,21832,29190', Triangle_Prob=0.9790006267951427)
Row(Triangle='18198,24730,32715', Triangle_Prob=0.9777015874196424)
Row(Triangle='3452,26319,34597', Triangle_Prob=0.9773423990391665)
Row(Triangle='1858,21716,44820', Triangle_Prob=0.9763981405377138)
Row(Triangle='20158,20893,32782', Triangle_Prob=0.9758026892636158)
Execution time : 105.80611968040466

2. `graphframe_fast.py`

```bash
spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_fast.py 10
```

Row(Triangle='13512,16091,20525', Triangle_Prob=0.9845106014259455)
Row(Triangle='32822,44147,46375', Triangle_Prob=0.9813710013541554)
Row(Triangle='1335,5686,18634', Triangle_Prob=0.9812171022432823)
Row(Triangle='38879,41049,43941', Triangle_Prob=0.9811032283661145)
Row(Triangle='1758,9788,40666', Triangle_Prob=0.9803237550574053)
Row(Triangle='167,21832,29190', Triangle_Prob=0.9790006267951427)
Row(Triangle='18198,24730,32715', Triangle_Prob=0.9777015874196424)
Row(Triangle='3452,26319,34597', Triangle_Prob=0.9773423990391665)
Row(Triangle='1858,21716,44820', Triangle_Prob=0.9763981405377138)
Row(Triangle='20158,20893,32782', Triangle_Prob=0.9758026892636158)
Execution time : 28.313462734222412

3. `rdd_bs.py`

```bash
spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_bs.py 10
```

('13512,16091,20525', 0.9845106014259455)
('32822,44147,46375', 0.9813710013541554)
('1335,18634,5686', 0.9812171022432822)
('38879,41049,43941', 0.9811032283661145)
('1758,40666,9788', 0.9803237550574053)
('167,21832,29190', 0.9790006267951427)
('18198,24730,32715', 0.9777015874196424)
('26319,3452,34597', 0.9773423990391665)
('1858,21716,44820', 0.9763981405377138)
('20158,20893,32782', 0.9758026892636158)
Execution time : 665.9714078903198

4. `rdd_fast.py`

```bash
spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_fast.py 10 0.8
```

('13512,16091,20525', 0.9845106014259455)
('32822,44147,46375', 0.9813710013541554)
('1335,18634,5686', 0.9812171022432822)
('38879,41049,43941', 0.9811032283661145)
('1758,40666,9788', 0.9803237550574053)
('167,21832,29190', 0.9790006267951427)
('18198,24730,32715', 0.9777015874196424)
('26319,3452,34597', 0.9773423990391665)
('1858,21716,44820', 0.9763981405377138)
('20158,20893,32782', 0.9758026892636158)
Execution time : 33.25533843040466


## About execution time

Execution time can be quite different from Run to Run for the same algorithm and the same dataset,
because of unrelatted processes running on parallel in the same hardware.

**Example 1:**

```bash
spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/rdd_bs.py 10
```

--> Run 1 | Execution time : 665.9714078903198
--> Run 2 | Execution time : 632.2999582290649
--> Run 3 | Execution time : 696.966732263565
--> Run 4 | Execution time : 692.2755947113037


**Example 2:**

```bash
spark-submit --master local[8] --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 src/graphframe_bs.py 10
```

--> Run 1 | Execution time : 99.53006553649902
--> Run 2 | Execution time : 100.28523015975952