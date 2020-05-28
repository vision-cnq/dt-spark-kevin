from pyspark import SparkConf, SparkContext

# 需要在project的project SDK中修改jdk为python3.5

def showResult(one):
    print(one)

if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("../resources/test.txt")
    words = lines.flatMap(lambda line:line.split(" "))
    partWords = words.map(lambda word:(word,1))
    reduceResult = partWords.reduceByKey(lambda v1,v2:v1+v2)
    reduceResult.foreach(lambda one:showResult(one))
