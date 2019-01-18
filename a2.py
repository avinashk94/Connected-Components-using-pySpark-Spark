from pyspark import SparkContext, SparkConf
import time
import sys

def vertex(line):
    l = line.split(" ")
    v = [int(x) for x in l]
    return [(v[0],v[1])]

def largeStar(v):
    return [(v[0],v[1]), (v[1],v[0])]

def largeStar2(v):
    min = v[0]
    for i in v[1]:
        if min > i:
            min = i
    list = []
    for i in v[1]:
        if v[0] < i:
            list.append((i,min))
        else:
            list.append((v[0],i))
    return list

def smallStar(v):
    if v[1] <= v[0]:
        list = [(v[0],v[1])]
    else:
        list = [(v[1],v[0])]
    return list

def smallStar2(v):
    min = v[0]
    for i in v[1]:
        if min > i:
            min = i
    list = []
    for i in v[1]:
        if v[0] > i and i!=min:
            list.append((i,min))
        else:
            list.append((v[0],i))
    return list

if __name__ == "__main__":
    conf = SparkConf().setAppName("Test1")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    lines = sc.textFile(sys.argv[1])
    V = lines.flatMap(vertex)
    print("Print1",V.collect())
    run = 1

    while run!=0:
        W = V
        V = V.flatMap(largeStar).groupByKey().map(lambda x : (x[0], list(x[1])))
        V = V.flatMap(largeStar2).distinct()
        print("Print After LargeStar",V.collect())
        V = V.flatMap(smallStar).groupByKey().map(lambda x : (x[0], list(x[1])))
        V = V.flatMap(smallStar2).distinct()
        print("Print After SmallStar",V.collect())
        run = W.subtractByKey(V).count()

    V.coalesce(1).saveAsTextFile("out")
    sc.stop()
