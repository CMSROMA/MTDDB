import os
import sys

inputfilepath = "/home/cmsdaq/MTDDB/uploader/files_to_upload/test_files/"
inputfilename = "Missing_Simulated_Galaxy_Bar_Bench_v1.csv"

inputfile = open(inputfilepath+"/"+inputfilename)
f_lines = inputfile.readlines()

currentarray = ""
for iline, line in enumerate(f_lines):
    if iline == 0:
        continue

    array = (line.split(",")[0]).split("_")[1]
    if array != currentarray:
        currentarray = array
        f = open(inputfilepath+"/"+inputfilename.replace(".csv", "")+"_"+currentarray+".csv", "a")
        f.write(f_lines[0])
        f.write(line)
    else:
        f.write(line)
    
