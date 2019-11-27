import sys, os

#this code list all the words which have length greater than threshold

#from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    folder = "weather/"
    years = []
    file_path = []
    hotDict = {'STN':[],'YEARMODA':[],'TEMP':[]}
    coldDict = {'STN':[],'YEARMODA':[],'TEMP':[]}

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

        cleanData = newInData.drop('_c1','_c4','_c5','_c6','_c7','_c8','_c10'
        ,'_c11','_c12','_c13','_c14','_c15','_c20','_c21')

        cleanData = cleanData.withColumnRenamed('_c0','STN')\
            .withColumnRenamed('_c2','YEARMODA').withColumnRenamed('_c3','TEMP')\
            .withColumnRenamed('_c9','STP').withColumnRenamed('_c16','GUST')\
            .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
            .withColumnRenamed('_c19','PRCP')

        cleanData = cleanData.withColumn("TEMP", cleanData["TEMP"].cast("float"))\
            .withColumn("PRCP", cleanData["PRCP"].cast("float"))

        cleanData.printSchema()
       
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
        
        #Station Code (MAX/MIN)
        station_code_max = max_row[0][0]
        station_code_min = min_row[0][0]

        #DATE (MAX/MIN)
        date_max = max_row[0][1]
        date_min = min_row[0][1]

        #UPDATE DICTIONARY HOT AND COLD
        hotDict["STN"].append(station_code_max[1::])
        hotDict["YEARMODA"].append(date_max[1::])
        hotDict["TEMP"].append(TEMP_MAX)
        coldDict["STN"].append(station_code_min[1::])
        coldDict["YEARMODA"].append(date_min[1::])
        coldDict["TEMP"].append(TEMP_MIN)
        
        print("Hottest LIST: " + str(hotDict))
        print("Coldest LIST: " + str(hotDict))

        #WRITE TO FILE
        print("Writing to File")
        f.write("\nYear: " + str(year) + " ---")

        #Hottest Day and Coldest Day
        f.write("\nHottest Day - " + str(date_max[4:6]) + "/" + str(date_max[6::]) + " with Station Code - " + str(station_code_max[1::]) + " and Temp of " + str(TEMP_MAX))
        f.write("\nColdest Day - " + str(date_min[4:6]) + "/" + str(date_min[6::]) + " with Station Code - " + str(station_code_min[1::]) + " and Temp of " + str(TEMP_MIN) + "\n")

    f.close()