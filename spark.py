import sys, os

#this code list all the words which have length greater than threshold

#from pyspark import SparkContext, SparkConf

def getMaxTemp(d):
    tempVal = d.get('TEMP')
    HOTEST = max(tempVal)
    maxIndex = tempVal.index(HOTEST)
    return HOTEST, maxIndex

def getMinTemp(d):
    tempVal = d.get('TEMP')
    COLDEST = min(tempVal)
    minIndex = tempVal.index(COLDEST)
    return COLDEST, minIndex

def getDate(d,index):
    # dateVal = d.get("YEARMODA")
    # date = dateVal[index]
    # return date
    return d.get("YEARMODA")[index]

def getStation(d,index):
    # stationVal = d.get("STN")
    # station = stationVal[index]
    # return station
    return d.get("STN")[index]

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
    for x in range(10):
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
        # print(names)

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
       
        ###### START CALCULATIONS ##########

        #Get Max and MIN TEMPS for each year
        TEMP_MAX = cleanData.select("TEMP").rdd.max()[0]
        TEMP_MIN = cleanData.select("TEMP").rdd.min()[0]
        year = years[x]
        print(year)


        # #Print Check
        # print("Max Temp for " + str(year) + " is: " + str(TEMP_MAX))
        # print("Min Temp for " + str(year) + " is: " + str(TEMP_MIN))

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

        # #WRITE TO FILE
        print("Writing to File\n")
        f.write("\nYear: " + str(year) + " ---")

        # #Hottest Day and Coldest Day of each year
        f.write("\nHottest Day - " + str(date_max[4:6]) + "/" + str(date_max[6::]) + " with Station Code - " + str(station_code_max[1::]) + " and Temp of " + str(TEMP_MAX))
        f.write("\nColdest Day - " + str(date_min[4:6]) + "/" + str(date_min[6::]) + " with Station Code - " + str(station_code_min[1::]) + " and Temp of " + str(TEMP_MIN) + "\n")

        ###################### TASK 2 ###############################################
        if (x == (len(file_path)-1)):

            #Hottest/Coldest Day from 2010-2019
            hotTemp, maxIndex = getMaxTemp(hotDict)
            coldTemp, minIndex = getMinTemp(coldDict)
            #get Days
            hotestDay = getDate(hotDict,maxIndex)
            coldestDay = getDate(coldDict,minIndex)
            #get Station Code
            hotStation = getStation(hotDict,maxIndex)
            coldStation = getStation(coldDict,minIndex)
            print(hotStation)
            print(coldStation)
            
            print("\nThe Hotest Day across all years (2010 - 2019) is: " + str(hotestDay[4:6]) + "/" + str(hotestDay[6::]) + " 2" + str(hotestDay[0:3]) + " with Station Code: " + str(hotStation) + " with Temp of " + str(hotTemp))
            print("\nThe Coldest Day across all years (2010 - 2019) is: " + str(coldestDay[4:6]) + "/" + str(coldestDay[6::]) + " 2" + str(coldestDay[0:3]) + " with Station Code: " + str(coldStation) + " with Temp of " + str(coldTemp))

        ###################### TASK 3 ###############################################

        if year == 2015:
            PRCP_MAX = cleanData.select("PRCP").rdd.max()[0]
            PRCP_MIN = cleanData.select("PRCP").rdd.min()[0]
            prcpmax_row = cleanData.filter(cleanData.TEMP == TEMP_MAX).collect()
            prcpmin_row = cleanData.filter(cleanData.TEMP == TEMP_MIN).collect()
             #Station Code (MAX/MIN)
            prcpstation_code_max = prcpmax_row[0][0]
            prcpstation_code_min = prcpmin_row[0][0]

            #DATE (MAX/MIN)
            prcpdate_max = prcpmax_row[0][1]
            prcpdate_min = prcpmin_row[0][1]

            f.write("\nMaximum Precipitation - " + str(prcpdate_max[4:6]) + "/" + str(prcpdate_max[6::]) + " with Station Code - " + str(prcpstation_code_max[1::]) + " and Precipation of " + str(PRCP_MAX))
            f.write("\nMinimum Precipitation - " + str(prcpdate_min[4:6]) + "/" + str(prcpdate_min[6::]) + " with Station Code - " + str(prcpstation_code_min[1::]) + " and Precipation of " + str(PRCP_MIN) + "\n")

        ###################### TASK 5 ###############################################

        if year == 2019:
            GUST_MAX = cleanData.select("GUST").rdd.max()[0]
            gustmax_row = cleanData.filter(cleanData.TEMP == TEMP_MAX).collect()
            guststation_code_max = prcpmax_row[0][0]
            gustdate_max = prcpmax_row[0][1]
            f.write("\nMaximum Wind Gust - " + str(gustdate_max[4:6]) + "/" + str(gustdate_max[6::]) + " with Station Code - " + str(guststation_code_max[1::]) + " and Gust Winds of " + str(GUST_MAX))

    f.close()