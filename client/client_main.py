import socket, time, atexit, csv, errno, random, os

# IP to name mapping
NAMES = {}
# Name to (IP,Port) mapping
ADDRESSES = {}
# Name to Node ID Mapping
NODE_ID = {}
global LEADER


def send_value(name, message):
  try:
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    send_socket.connect(ADDRESSES[name])
    send_socket.send(message)
    send_socket.close()
  except socket.error, e:
    err =  e.args[0]
    if err in [errno.ECONNREFUSED,errno.ETIMEDOUT]:
      return e.args[0]
    else:
      print e
      sys.exit(1)

  data = None

  recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  recv_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
  recv_socket.bind((socket.gethostname(), 10010))
  recv_socket.setblocking(0)
  recv_socket.listen(1)
  while data == None:
    try:
      c, address = recv_socket.accept()
      data = c.recv(4096)
      c.close()
    except socket.error, e:
      err = e.args[0]
      if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
        return None
  recv_socket.close()
  return data

def get_server_info(numNodes):
  global LEADER
  with open('../settings/peernames%d.csv'%numNodes, 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
      ADDRESSES[row[0]] = (row[1], int(row[2]))
      NAMES[row[1]] = row[0]
      NODE_ID[row[0]] = int(row[3])

  leader_id = max(NODE_ID.values())
  for name,nid in NODE_ID.items():
    if nid == leader_id:
      LEADER = name

def load_settings(setting_files):
  settings = {}
  for s in setting_files:
    if os.path.exists(s):
      with open(s, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
          settings[row[0]] = int(row[1])
  return settings

def write_log(name, value):
  with open("log.csv","a+") as f:
    f.write("%s,%s\n"%(name, value))

def process_input(loadedIndex):
  global LEADER
  MessageId = loadedIndex+1

  while(1):
    value = raw_input("New Value: ")
    message = ":".join([str(MessageId),value])
    sending = True
    while(sending):
      data = send_value(LEADER, message)
      if data == errno.ECONNREFUSED:
        LEADER = random.sample(set(NAMES.values())-set([LEADER]),1)[0]
      elif len(data) > 1:
        if data[0] == 'R':
          data = data.split(":")
          cvalue = data[-1]
          if cvalue == value:
            print "Value Chosen"
            sending = False
          else:
            MessageId += 1
            write_log("largestMessageId", str(MessageId))
            message = ":".join([str(MessageId),value])
        else:
          LEADER = data
    MessageId += 1

if __name__ == "__main__":
  settings = load_settings(["../settings/settings.csv","log.csv"])
  get_server_info(settings["numNodes"])

  id = 0
  if "largestMessageId" in settings.keys():
    id = int(settings["largestMessageId"])
  process_input(id)
