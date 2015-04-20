import subprocess
import os

# do the test
cmd = 'java Cloud 15640 db1.txt u-110-130-15640 8 3 > res.txt'
os.system(cmd)

print 'program done, analyzing results'
#analyze result data

def analyze():
    with open('res.txt') as f:
        lines = f.readlines()
        for strline in lines:
            if strline.startswith('Total VM time'):
                length = len(strline)
                vmtime = int(strline[15: length])
                print vmtime
            elif strline.startswith('Stats'):
                length = len(strline)
                stats = strline[8: length-1]
                print stats

analyze()




# Total VM time
# Stats	