import sys, os
from pyspark.sql import functions as F

#this code list all the words which have length greater than threshold

#from pyspark import SparkContext, SparkConf

#GET MAX TEMP
def getMaxTemp(d):
    tempVal = d.get('TEMP')
    HOTEST = max(tempVal)
    maxIndex = tempVal.index(HOTEST)
    return HOTEST, maxIndex

#GET MIN TEMP
def getMinTemp(d):
    tempVal = d.get('TEMP')
    COLDEST = min(tempVal)
    minIndex = tempVal.index(COLDEST)
    return COLDEST, minIndex

#GET DATE
def getDate(d,index):
    return d.get("YEARMODA")[index]

#GET STATION
def getStation(d,index):
    return d.get("STN")[index]

if __name__ == "__main__":
    #Initialize
    folder = "weather/"
    years = []
    file_path = []
    hotDict = {'STN':[],'YEARMODA':[],'TEMP':[]}
    coldDict = {'STN':[],'YEARMODA':[],'TEMP':[]}

    #SET PATHS and create result.txt
    og_path = os.getcwd()
    f = open(os.path.join(og_path,"result.txt"),'w+')
    f.write("GLOBAL SURFACE SUMMARY \n")
    f.close()

    #Open new file
    f = open(os.path.join(og_path,"result.txt"),'a+')

    #Create list of years
    for x in range(10):
        year = "201" + str(x)
        years.append(year)

    for year in range(len(years)):
        y = years[year]
        file_path.append(folder + y)

    #For 2010 - 2019 
    for x in range(len(file_path)):
        #x represents the year
        x = 5
        subfolder = file_path[x]
        print("\nGot Weather Data from: " + subfolder)
        print("RUNNING Spark on: " + subfolder)

        #Read Data
        inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(subfolder)

        #Convert headers to appropriate schema
        name_list = inTextData.schema.names
        name_list = str(name_list).strip("['']").split(' ')
        names = []
        for item in name_list:
            if len(item)>0:
                names.append(item)
        print(names)

        ##RDD SECTION - Format data
        rdd1 = inTextData.rdd
        rdd2 = rdd1.map(lambda x: str(x).replace("'", ""))
        rdd3 = rdd2.map(lambda x: str(x).replace("*", ""))
        rdd4 = rdd3.map(lambda x: str(x).split('=')[1])
        rdd5 = rdd4.map(lambda x: ' '.join(x.split()))
        rdd6 = rdd5.map(lambda x: str(x).replace("A", ""))
        rdd7 = rdd6.map(lambda x: str(x).replace("B", ""))
        rdd8 = rdd7.map(lambda x: str(x).replace("C", ""))
        rdd9 = rdd8.map(lambda x: str(x).replace("D", ""))
        rdd10 = rdd9.map(lambda x: str(x).replace("E", ""))
        rdd11 = rdd10.map(lambda x: str(x).replace("F", ""))3
        rdd12 = rdd11.map(lambda x: str(x).replace("G", ""))
        rdd13 = rdd12.map(lambda x: str(x).replace("H", ""))
        rdd14 = rdd13.map(lambda x: str(x).replace("I", ""))
        rdd15 = rdd14.map(lambda x: ' '.join(x.split()))
        # rdd7 = rdd6.map(lambda x: )

        #Set temp data
        newDataFolder = subfolder + "/temp"

        #Delete temp folder
        os.system("hadoop fs -rm -r " + newDataFolder)

        #save data as txt
        rdd15.saveAsTextFile(newDataFolder)

        #create new temp folder
        newInData = spark.read.csv(newDataFolder,header=False,sep=' ')

        #Drop columns
        cleanData = newInData.drop('_c1','_c4','_c5','_c6','_c7','_c8','_c10'
        ,'_c11','_c12','_c13','_c14','_c15','_c20','_c21')

        #Rename Columns
        cleanData = cleanData.withColumnRenamed('_c0','STN')\
            .withColumnRenamed('_c2','YEARMODA').withColumnRenamed('_c3','TEMP')\
            .withColumnRenamed('_c9','STP').withColumnRenamed('_c16','GUST')\
            .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
            .withColumnRenamed('_c19','PRCP')

        #replace
        # cleanData = cleanData.na.replace(['G'],'','PRCP')
        # newDf = cleanData.withColumn('PRCP',regexp_replace('PRCP','G',''))

        #Set columns to different type
        cleanData = cleanData.withColumn("TEMP", cleanData["TEMP"].cast("double"))\
            .withColumn("GUST", cleanData["GUST"].cast("double"))\
            .withColumn("MAX", cleanData["MAX"].cast("double"))\
            .withColumn("MIN", cleanData["MIN"].cast("double"))\
            .withColumn("PRCP", cleanData["PRCP"].cast("double"))


            # .withColumn("PRCP", cleanData["PRCP"].cast("float"))\

        cleanData = cleanData.where("MAX!=9999.9")
        cleanData.printSchema()
       
        ###### START CALCULATIONS ##########

        #Get Max and MIN TEMPS for each year
        TEMP_MAX = cleanData.select("MAX").rdd.max()[0]
        TEMP_MIN = cleanData.select("MIN").rdd.min()[0]
        year = years[x]


        # #Print Check
        # print("Max Temp for " + str(year) + " is: " + str(TEMP_MAX))
        # print("Min Temp for " + str(year) + " is: " + str(TEMP_MIN))

        #Get Station Code and Data 
        max_row = cleanData.filter(cleanData.MAX == TEMP_MAX).collect()
        min_row = cleanData.filter(cleanData.MIN == TEMP_MIN).collect()
        
        #Station Code (MAX/MIN)
        station_code_max = max_row[0][0]
        station_code_min = min_row[0][0]

        #DATE (MAX/MIN)
        date_max = max_row[0][1]
        date_min = min_row[0][1]

        #UPDATE DICTIONARY HOT AND COLD
        hotDict["STN"].append(station_code_max)
        hotDict["YEARMODA"].append(date_max[1::])
        hotDict["TEMP"].append(TEMP_MAX)
        coldDict["STN"].append(station_code_min)
        coldDict["YEARMODA"].append(date_min[1::])
        coldDict["TEMP"].append(TEMP_MIN)
        
        print("Hottest LIST: " + str(hotDict))
        print("Coldest LIST: " + str(hotDict))

        # #WRITE TO FILE
        print("Writing to File\n")
        f.write("\nYear: " + str(year) + " ---")

        # #Hottest Day and Coldest Day of each year
        print("\nHottest Day - " + str(date_max[4:6]) + "/" + str(date_max[6::]) + " with Station Code - " + str(station_code_max) + " and Temp of " + str(TEMP_MAX))
        print("\nColdest Day - " + str(date_min[4:6]) + "/" + str(date_min[6::]) + " with Station Code - " + str(station_code_min) + " and Temp of " + str(TEMP_MIN) + "\n")
        f.write("\nHottest Day - " + str(date_max[4:6]) + "/" + str(date_max[6::]) + " with Station Code - " + str(station_code_max) + " and Temp of " + str(TEMP_MAX))
        f.write("\nColdest Day - " + str(date_min[4:6]) + "/" + str(date_min[6::]) + " with Station Code - " + str(station_code_min) + " and Temp of " + str(TEMP_MIN) + "\n")

        ###################### TASK 2 - HOTEST AND COLDEST DAY ALL YEARS ###############################################
        if (x == (len(file_path)-1)):
            print("TASK 2")
            f.write("\nTask 2")
            #Hottest/Coldest Day from 2010-2019
            hotTemp, maxIndex = getMaxTemp(hotDict)
            coldTemp, minIndex = getMinTemp(coldDict)
            #get Days
            hotestDay = getDate(hotDict,maxIndex)
            coldestDay = getDate(coldDict,minIndex)
            #get Station Code
            hotStation = getStation(hotDict,maxIndex)
            coldStation = getStation(coldDict,minIndex)
            
            print("HOTEST AND COLD Days of all years\n")
            print("\nThe Hotest Day across all years (2010 - 2019) is: " + str(hotestDay[3:5]) + "/" + str(hotestDay[5::]) + " 2" + str(hotestDay[0:3]) + " with Station Code: " + str(hotStation) + " with Temp of " + str(hotTemp))
            print("\nThe Coldest Day across all years (2010 - 2019) is: " + str(coldestDay[3:5]) + "/" + str(coldestDay[5::]) + " 2" + str(coldestDay[0:3]) + " with Station Code: " + str(coldStation) + " with Temp of " + str(coldTemp))

            f.write("\nThe Hotest Day across all years (2010 - 2019) is: " + str(hotestDay[3:5]) + "/" + str(hotestDay[5::]) + " 2" + str(hotestDay[0:3]) + " with Station Code: " + str(hotStation) + " with Temp of " + str(hotTemp))
            f.write("\nThe Coldest Day across all years (2010 - 2019) is: " + str(coldestDay[3:5]) + "/" + str(coldestDay[5::]) + " 2" + str(coldestDay[0:3]) + " with Station Code: " + str(coldStation) + " with Temp of " + str(coldTemp) + "\n")

        # ###################### TASK 3 - MAX AND MIN PRCP ###############################################
        if(year == '2015'):
            print("CALCULATING Precipitation in 2015\n")
            f.write("\nTask 3")
            newData = cleanData.where("PRCP != 99.99")
            PRCP_MAX = newData.select("PRCP").rdd.max()[0]
            PRCP_MIN = newData.select("PRCP").rdd.min()[0]
            prcpmax_row = newData.filter(newData.PRCP == PRCP_MAX).collect()
            prcpmin_row = newData.filter(newData.PRCP == PRCP_MIN).collect()

             #Station Code (MAX/MIN)
            prcpstation_code_max = prcpmax_row[0][0]
            prcpstation_code_min = prcpmin_row[0][0]

            #DATE (MAX/MIN)4
            prcpdate_max = prcpmax_row[0][1]
            prcpdate_min = prcpmin_row[0][1]

            print("\nMaximum Precipitation - " + str(prcpdate_max[4:6]) + "/" + str(prcpdate_max[6::]) + " with Station Code - " + str(prcpstation_code_max) + " and Precipation of " + str(PRCP_MAX))
            print("\nMinimum Precipitation - " + str(prcpdate_min[4:6]) + "/" + str(prcpdate_min[6::]) + " with Station Code - " + str(prcpstation_code_min) + " and Precipation of " + str(PRCP_MIN) + "\n")

            f.write("\nMaximum Precipitation - " + str(prcpdate_max[4:6]) + "/" + str(prcpdate_max[6::]) + " with Station Code - " + str(prcpstation_code_max) + " and Precipation of " + str(PRCP_MAX))
            f.write("\nMinimum Precipitation - " + str(prcpdate_min[4:6]) + "/" + str(prcpdate_min[6::]) + " with Station Code - " + str(prcpstation_code_min) + " and Precipation of " + str(PRCP_MIN) + "\n")


        if (year == '2019'):
            ###################### TASK 4 - MISSING VALUES ###############################################
            #Count percentage missing values for mean station pressure (STP) for year 2019 and stations.
            print("TASK 4")
            f.write("\nTASK 4")
            missing = cleanData.where("STP = 9999.9").count()
            totalSTP = cleanData.count()
            pctMissing =  float(missing) / float(totalSTP) * 100
            # str({"2f"}.format(pctMissing))
            print("\nThe Percent of Missing Values for STP for 2019 is " + str(float("{0:.2f}".format(pctMissing))) + "%")
            f.write("\nThe Percent of Missing Values for STP for 2019 is " + str(pctMissing) + "%"  + "\n")

            ###################### TASK 5 - MAX WIND GUST###############################################
            print("TASK 5")
            f.write('\nTask 5')
            GustData = cleanData.where("GUST!=999.9")
            GUST_MAX = GustData.select("GUST").rdd.max()[0]
            gustmax_row = GustData.filter(GustData.GUST == GUST_MAX).collect()
            guststation_code_max = gustmax_row[0][0]
            gustdate_max = gustmax_row[0][1]
            print("\nMaximum Wind Gust - " + str(gustdate_max[4:6]) + "/" + str(gustdate_max[6::]) + " with Station Code - " + str(guststation_code_max) + " and Gust Winds of " + str(GUST_MAX) + " knots")
            f.write("\nMaximum Wind Gust - " + str(gustdate_max[4:6]) + "/" + str(gustdate_max[6::]) + " with Station Code - " + str(guststation_code_max) + " and Gust Winds of " + str(GUST_MAX) + " knots" + "\n")

    f.close()