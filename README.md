# K-Means-X-MapReduce
A project implementing K-Means clustering algorithm in Map-Reduce using sythetic data as a sample.

## PointsGenerator.py
A python file that generated random points in the form of x,y following skewed distribution towards 0.

exec: 
```console
$python3   PointsGenerator.py    [-n NUM]     out

positional arguments: 
  out                   output file
  
optional arguments:
  -n NUM, --number      number of points to generate  (default: 1.000.000)
```
*this program must run with python3


## KMeans.jar

```console
$bin/hadoop    jar     KMeans.jar     KMeans    input_dir     output_dir
```
output_dir   must not exist, it will be generated by the program.
