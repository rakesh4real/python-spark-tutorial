from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("wordCounts").setMaster("local[3]")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("in/word_count.text")
    wordRdd = lines.flatMap(lambda line: line.split(" "))
    wordPairRdd = wordRdd.map(lambda word: (word, 1))

    wordCounts = wordPairRdd.reduceByKey(lambda x, y: x + y)
    for word, count in wordCounts.collect(): #cant use 'items' as it can only be used for actions like wordCounts. 'Items' cant be used with lists.. for lists, directly take it
        print("{} : {}".format(word, count))
