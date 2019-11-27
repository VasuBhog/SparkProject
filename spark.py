import sys, os

#this code list all the words which have length greater than threshold

#from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    folder = "weather/"
    years = []
    file_path = []

    og_path = os.getcwd()
    f = open(os.path.join(og_path,"result.txt"),'w+')
    f.close()

    f = open(os.path.join(og_path,"result.txt"),'a+')
    #Testing on 2 subfolders - should be 10
    for x in range(2):
        year = "201" + str(x)
        years.append(year)

    for year in range(len(years)):
        y = years[year]
        file_path.append(folder + y)


    for x in range(len(file_path)):
        subfolder = file_path[x]
        print("\nGot Weather Data from: " + subfolder)
        print("RUNNING Spark on: " + subfolder)
        inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(subfolder)

        inTextData.count()
        name_list = inTextData.schema.names
        name_list = str(name_list).strip("['']").split(' ')
        # print(name_list)
        names = []
        for item in name_list:
            if len(item)>0:
                names.append(item)
        print(names)

        #rdd
        rdd1 = inTextData.rdd
        # rdd1.take(2)
        rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
        # rdd2.take(4)
        rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
        rdd4 = rdd3.map(lambda x: x[1:-2])
        # rdd4.take(4)
        rdd4.saveAsTextFile(subfolder+'temp')

        
        newInData = spark.read.csv(subfolder+'temp',header=False,sep=' ')
        # newInData.show(10)
        cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
        # cleanData.show(5)
        cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
        .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
        .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
        .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
        .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
        .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
        .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
        .withColumnRenamed('_c21','FRSHTT')

        cleanData = cleanData.withColumn("TEMP", cleanData["TEMP"].cast("float"))\
            .withColumn("PRCP", cleanData["PRCP"].cast("float"))

        cleanData.printSchema()
        # cleanData.show(2,False)

        # tempDf = cleanData.select(cleanData.TEMP.cast("float"),cleanData.PRCP.cast("float"))

        #Get Max and MIN TEMPS for each year
        TEMP_MAX = cleanData.select("TEMP").rdd.max()[0]
        TEMP_MIN = cleanData.select("TEMP").rdd.min()[0]
        year = years[x]

        #Print Check
        print("Max Temp for " + str(year) + " is: " + str(TEMP_MAX))
        print("Min Temp for " + str(year) + " is: " + str(TEMP_MIN))

        #Get Station Code and Data
        max_row = cleanData.filter(cleanData.TEMP == TEMP_MAX).collect()
        min_row = cleanData.filter(cleanData.TEMP == TEMP_MIN).collect()
        
        #max
        station_code_max = max_row[0][0]
        date_max = max_row[0][1]

        #min
        station_code_min = min_row[0][0]
        date_min = min_row[0][1]


        #WRITE TO FILE
        print("Writing to File")
        f.write("\nYear: " + str(year) + "---")

        f.write("\nMax Temp for Station Code - " + str(station_code_max[1::]) + "and Date - " + str(date_max) + " is " + str(TEMP_MAX))
        f.write("\nMin Temp for Station Code - " + str(station_code_min[1::]) + "and Date - " + str(date_min) + " is " + str(TEMP_MIN) + "\n")

    f.close()