import pyspark
import pytz, time
from datetime import datetime

def sortByResponseTime(line):
    dateTime = line.split(" ")[3] + "]"
    dateTime = datetime.strptime(dateTime, "[%d/%b/%Y:%H:%M:%S]")
    dateTime = pytz.timezone("Asia/Saigon").localize(dateTime)
    timestamp = time.mktime(dateTime.timetuple()) + dateTime.microsecond/1e6
    return timestamp

def Validate(row):
    components = row.split(" ")
    valid = True
    if len(components) != 7: #not to have enough field
        valid = False
    else:
        response_time = float(components[0]) #convert string to float
        size = int(components[6]) #convert string to ine
        cache_type = str(components[2])
        if  response_time <= 0.0 or size <= 0:
            valid = False
        elif cache_type == '-':
            valid = False
    return valid
if __name__ == "__main__":
    log = pyspark.SparkContext('local[*]').textFile("hdfs:///user/loclh/data/system_records.log", 3) #get file with 3 is minPartition
    validateLogs = log.filter(Validate)
    sortedLogs = validateLogs.sortBy(sortByResponseTime)
    print("number of record in log file ",log.count())
    print("number of validate record in log file ",validateLogs.count())
    for record in sortedLogs.take(10):
        print("===============NUMBER THE MOST OF LATE RESPONSE================")
        print(record)
        print("===============================================================")
