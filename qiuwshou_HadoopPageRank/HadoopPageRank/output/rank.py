import operator

f = open('part-r-00000', "r")
lines = f.readlines()
record = {}
for line in lines:
    strArray = line.split("	")
    url = int(strArray[0])
    rank = round(float(strArray[1]),6)
    record[url] = rank
sorted_record = sorted(record.items(), key = operator.itemgetter(1))
output = open("qiuwshou_HadoopPageRank_output.txt", "w")
count = 1
for r in reversed(sorted_record):
    output.write(str(count) + "  "+ str(r))
    output.write("\n")
    count += 1
output.close()


