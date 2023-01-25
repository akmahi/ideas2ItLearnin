import csv
import json
detail_list =[]

with open ("C:/Users/Windows/Documents/WorkStore/basicDetails.csv") as details:
	split_details = csv.reader(details, delimiter=",")
	header_line = 0
	for row in split_details:
		if header_line > 0:
			detail_list.append({
				"name": row[1] + row[2],  "dob": row[3], "address":{"city":row[4], "state": row[5], "country": row[6]},
				"contact" : {"email":row[7], "phone":row[8]}})
		header_line += 1
with open("details.json", "w") as json_file:
	json.dump(detail_list, json_file)
	
			