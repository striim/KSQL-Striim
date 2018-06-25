import sys, csv ,operator
#this script sorts the lineitem data set based on the orderdate time, essentially we can add a time to lineitem and order by that. 
#we need the timestamp to convert the workload to streaming
data = csv.reader(open('lineitem1.csv'),delimiter=',')
sortedlist = sorted(data, key=operator.itemgetter(15))  
#now write the sorte result into new CSV file
with open("lineitem1_sorted.csv", "wb") as f:
	fileWriter = csv.writer(f, delimiter=',')
	for row in sortedlist:
		fileWriter.writerow(row)
