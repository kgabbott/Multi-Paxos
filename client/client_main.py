import socket, time, atexit, csv, errno

# IP to name mapping
NAMES = {}
# Name to (IP,Port) mapping
ADDRESSES = {}
# Name to Node ID Mapping
NODE_ID = {}
LEADER = None

def send_message(name, message):
  send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  send_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
  send_socket.connect(ADDRESSES[name])
  send_socket.send(message)
  send_socket.close()

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
  print "LEADER is [NODE %d: %s]" %(NODE_ID[LEADER], LEADER)

def load_settings(setting_files):
  settings = {}
  for s in setting_files:
    with open(s, 'rb') as f:
      reader = csv.reader(f)
      for row in reader:
        settings[row[0]] = int(row[1])
  return settings

if __name__ == "__main__":
  settings = load_settings(["../settings/settings.csv","../settings/client_settings.csv"])
  get_server_info(settings["numNodes"])

  while(1):
    input = raw_input("New Value: ")
    for name in NAMES.values():
      data = send_message(name, input)
      if data == "Processing":
        print "%s is Processing value" % name
      else:
        print "%s says leader is %s" % (name, data)


