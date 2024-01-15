from pyspark import SparkContext
import sys

def main():
    #create a sparkcontext
    sc=SparkContext(appName='averagevis')

    # import the input file
    input_rdd = sc.textFile(''/user/hadoop/inputproject1/Project_Data/*.gz')

    # filter out records with invalid quality values and temp values
    filtered_rdd = input_rdd.filter(lambda line: line[79:85] != "999999" and int(line[85:86]) in [1, 4, 5, 0, 9])

    # Extract the USAF weather station ID and visibility value for each record
    station_vis_rdd = filtered_rdd.map(lambda line: (line[5:11], int(line[79:85])))

    # Group the visibility values by station ID
    grouped_rdd = station_vis_rdd.groupByKey()

    # Compute the average visibility distance for each station ID
    avg_vis_rdd = grouped_rdd.map(lambda pair: (pair[0], sum(pair[1]) / len(pair[1])))

    # save the output in  a output file
    avg_vis_rdd.saveAsTextFile('/user/hadoop/outputproject1/')


if __name__ == '__main__':
    main()