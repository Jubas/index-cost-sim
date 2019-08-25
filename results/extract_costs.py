import re

filename = "OPTMEM_II_Read.log"
out_name = "OPTMEM_II_Read.csv"

cost_rec = re.compile('(\d+)\t([a-zA-Z0-9_]+)\t(\d+.\d+)\t([a-zA-Z_]+_Cost)\t(\d+.\d+)')


lines = []
lines.append("Device\tInserts (in millions)\tCost (in hours)\n")
with open(filename, 'r') as f:
    for line in f.readlines():
        matches = cost_rec.search(line)
        if matches:
            IOtype_cost = matches.group(4)
            if 'IO_Total_Total_Cost' in IOtype_cost:
                split = line.split("\t")
                formatted_inserts = str(int(float(split[2])))
                new_line = ("%s\t%s\t%s" % (split[1], formatted_inserts, split[4]))
                lines.append(new_line)

with open(out_name, "w") as f:
    for item in lines:
        f.write("%s" % item)