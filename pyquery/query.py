import time
from pprint import pprint


from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery

cb = Bucket('couchbase://172.31.8.92:8091/stats')

query_string = 'select kingdom as metric, phylum as subtype, `count` as timeMsec from stats where type="m_table_count_debug" and kingdom in ["join_ack_time", "ws_create_time", "chaal_time_new", "player_load"] and  phylum > 0 and phylum < 10 and game_id=3 and  `count` > 0 and timestamp > ($start) and timestamp < ($end)  order by kingdom, phylum, `count`'

def percentileN(numbers, l, n) :
	i = l*n/100 - 1

	return numbers[i]

ts = int(time.time())
startTS = ts - 90
endTS = ts - 30 # 30 second lag

metric = ""
currentMetric = ""
metricMap = {}
percentileResults = {}
currentSubType = 1
numbers = []

print (' start time ', startTS, 'end time ', endTS)
query = N1QLQuery(query_string, start=startTS, end=endTS)
for row in cb.n1ql_query(query):
	#pprint(row)
	metric = row["metric"]
	if currentMetric == "" :
		metricMap[metric] = percentileResults
		currentMetric = metric
	elif currentMetric != metric:
		metricMap[currentMetric] = dict(percentileResults)
		percentileResults = {}
		metricMap[metric] = percentileResults
		currentMetric = metric
	

	subType = int(row["subtype"])
	if subType != currentSubType :
		# roll-over the subType and calculate the percentile for the
		# current set of numbers
		subTypeMap = {}
		subTypeMap["50th percentile"] = percentileN(numbers, len(numbers), 50)
		subTypeMap["80th percentile"] = percentileN(numbers, len(numbers), 80)
		subTypeMap["90th percentile"] = percentileN(numbers, len(numbers), 90)
		percentileResults[currentSubType] = subTypeMap

		numbers = []
		currentSubType = subType

	else :
		timeMsec = int(row["timeMsec"])
		numbers.append(timeMsec)

if len(numbers) > 0 :
	subTypeMap = {}
	subTypeMap["50th percentile"] = percentileN(numbers, len(numbers), 50)
	subTypeMap["80th percentile"] = percentileN(numbers, len(numbers), 80)
	subTypeMap["90th percentile"] = percentileN(numbers, len(numbers), 90)
	percentileResults[currentSubType] = subTypeMap

pprint(metricMap)



