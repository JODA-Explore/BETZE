
import sys


file_index = sys.argv[1]

files = []
with open(file_index, "r") as a_file:
  for line in a_file:
    files.append(line.strip())


depth_map = {}
attribute_map = {}
total_count = 0
total_queries = 0

top_list = [5,10,20]

def extract_attributes(text):      
  import re
  matches = re.findall(r"'(.+?)'",text)
  return matches

for file in files:
  # Iterate over the file line by line
  with open(file, "r") as a_file:
    for line in a_file:
      stripped_line = line.strip()

      if stripped_line.startswith("LOAD"):
        total_queries += 1

      # Find all strings surrounded by '
      attributes = extract_attributes(stripped_line)
      for (i, attribute) in enumerate(attributes):
        total_count += 1
        # If the attribute is not in the attribute map, add it
        if attribute not in attribute_map:
          attribute_map[attribute] = 1
        else:
          attribute_map[attribute] += 1
        
        # Get depth
        depth = attribute.count('/')
        if depth not in depth_map:
          depth_map[depth] = 1
        else:
          depth_map[depth] += 1
print("--- Stats ---")
# Print total count and size of files
print("Total files: {}".format(len(files)))
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
  if(attribute_map[attribute] > 3):
    print(attribute, "\t", attribute_map[attribute], "\t", "{:.2f}%".format((attribute_map[attribute]/total_count)*100) )

  if i in top_list:
    print("---- Top-{}: {} ({:.2f}%)----".format(i, top_sum,(top_sum/total_count)*100))