
import json
import sys


betze_file = sys.argv[1]



depth_map = {}
attribute_map = {}
function_map = {}
total_count = 0
total_queries = 0

top_list = [5,10,20]

def extract_attributes(param):      
  if "Path" in param:
    attr = param["Path"]
    if attr not in attribute_map:
      attribute_map[attr] = 1
    else:
      attribute_map[attr] += 1
    # Get depth
    depth = attr.count('/')
    if depth not in depth_map:
      depth_map[depth] = 1
    else:
      depth_map[depth] += 1
    

def extract_functions(filter):
  if filter["type"] == "OrPredicate" or filter["type"] == "AndPredicate":
    extract_functions(filter["parameter"]["Lhs"])
    extract_functions(filter["parameter"]["Rhs"])
  elif filter["type"] == "NotPredicate":
    extract_functions(filter["parameter"])
  else:
    global total_count
    total_count += 1
    if filter["type"] not in function_map:
      function_map[filter["type"]] = 1
    else:
      function_map[filter["type"]] += 1
    extract_attributes(filter["parameter"])


# Iterate over the file line by line
with open(betze_file, "r") as a_file:
  betze = json.load(a_file)
  for query in betze["queries"]:
    extract_functions(query["filter"])

    total_queries += 1
      
    
print("--- Stats ---")
# Print total count and size of files
print("Total attributes: {}".format(total_count))
print("Total queries: {}".format(total_queries))

print("--- Depth\tCount\t% ---")
for depth in {k: v for k, v in sorted(depth_map.items(), key=lambda item: item[0])}:
  print(depth, "\t", depth_map[depth], "\t", "{:.2f}%".format((depth_map[depth]/total_count)*100) )


print("--- Attribute\tCount ---")
i = 0
top_sum = 0
for attribute in {k: v for k, v in sorted(attribute_map.items(), key=lambda item: item[1], reverse=True)}:
  i+=1
  top_sum += attribute_map[attribute]
  if(attribute_map[attribute]):
    print(attribute, "\t", attribute_map[attribute], "\t", "{:.2f}%".format((attribute_map[attribute]/total_count)*100) )

  if i in top_list:
    print("---- Top-{}: {} ({:.2f}%)----".format(i, top_sum,(top_sum/total_count)*100))

print("--- Functions ---")
for function in {k: v for k, v in sorted(function_map.items(), key=lambda item: item[1], reverse=True)}:
  print(function, "\t", function_map[function], "\t", "{:.2f}%".format((function_map[function]/total_count)*100) )