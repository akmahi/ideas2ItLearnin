import csv
import json
detail_list =[]
contact_keys = {"email", "phone"}
address_keys = {"city", "state", "country"}

with open ("C:/Users/Windows/Documents/WorkStore/basicDetails.csv") as details:
	split_details = csv.DictReader(details, delimiter=",")
	for row in split_details:
		base_json = {"contact":{},"address":{}, "name":"", "dob":""}
		identification = {"first_name":"", "last_name":"", "dob":""}
		for key in row.keys():
			if key in contact_keys:
				base_json["contact"][key] = row.get(key,"")
			elif key in address_keys:
				base_json["address"][key] = row.get(key,"")
			elif key in identification:
				identification[key] = row.get(key,"")
		base_json["name"] = identification.get("first_name", "") + " " + identification.get("last_name", "")
		base_json["dob"] = identification.get("dob", "")
		detail_list.append(base_json)
with open("details_new.json", "w") as json_file:
	json.dump(detail_list, json_file)
print(detail_list)
				 
				
		
		
	
			