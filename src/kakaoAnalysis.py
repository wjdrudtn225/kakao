from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("kakao")
sc = SparkContext(conf=conf)
#위의 3줄은 pyspark를 사용하기 위한 준비

rdd = sc.textFile("kakao.txt").map(lambda x: x.split(']')[0])
#']'를 기준으로 0번째에 해당하는 kakao.txt값을 가져온다

n=0
#평균을 내기위해, 전체 값을 n에 저장할 것이다.

for i in rdd.collect():
        n = n+1
#n을 구하는 과정

sumrdd = rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
#reduceByKey를 사용하기 위해 (key,1) 꼴로 만들어준다.

avrrdd = sumrdd.map(lambda x: (x[0],x[1]/n*100))
#(key,%) 형태로 만들어준다.

for i in avrrdd.collect():
        print(i)
#1자로 출력한다. 참고로 그냥 이어지게 출력하려면,
#print(avrrdd.collect())를 사용하면된다.