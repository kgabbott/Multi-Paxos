import csv, socket

NAMES = {}
# Name to (IP,Port) mapping
ADDRESSES = {}
# Name to Node ID Mapping
NODE_ID = {}
# Name to status mapping (0=offline, 1=online,2=leader)
STATUS = {}

name = []
ips = []
hostname = socket.gethostname().split(".")[0]

with open('peernames.csv', 'rb') as f:
  reader = csv.reader(f)
  for row in reader:
    ADDRESSES[row[0]] = (row[1], int(row[2]))
    ip = int(row[1].split('.')[-1])
    name.append(row[0])
    ips.append(ip)
    if(row[0] != hostname):
      NAMES[row[1]] = row[0]

sorted_ips = sorted(ips)
for i in range(0,len(name)):
  NODE_ID[name[i]] = sorted_ips.index(ips[i])+1

start_leader = name[ips.index(max(ips))]
print start_leader


