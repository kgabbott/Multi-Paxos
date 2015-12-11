import socket, time, atexit, csv, errno, random, os, sys, signal

# IP to name mapping
NAMES = {}
# Name to (IP,Port) mapping
ADDRESSES = {}
# Name to Node ID Mapping
NODE_ID = {}

CHOSEN = 0
global LEADER


def send_value(name, message, requestTimeout):
  try:
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    send_socket.connect(ADDRESSES[name])
    send_socket.send(message)
    send_socket.close()
  except socket.error, e:
    err =  e.args[0]
    if err in [errno.ECONNREFUSED,errno.ETIMEDOUT]:
      return errno.ETIMEDOUT
    else:
      print e
      sys.exit(1)

  data = None
  t = time.time()

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
      if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
        if t - time.time() > requestTimeout:
          recv_socket.close()
          return errno.ETIMEDOUT
      else:
        recv_socket.close()
        print e
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
          if row[0] == "Chosen":
            if "Chosen" in settings.keys() and int(row[1]) > settings[row[0]]:
              settings[row[0]] = int(row[1])
            else:
              settings[row[0]] = int(row[1])
          else:
            settings[row[0]] = int(row[1])
  return settings

def write_log(value):
  with open("log.csv","a+") as f:
    f.write("%s\n"%value)

def process_input(loadedIndex, requestTimeout):
  global LEADER, CHOSEN
  CHOSEN = loadedIndex
  MessageId = loadedIndex+1

  while(1):
    value = ""
    try:
      valid_functions = ["add", "get", "remove"]
      while value not in valid_functions:
        function = raw_input("Function (add/get/remove): ")
        value = function
      value += " "+raw_input("Key (cannot contain ':' or ' '): ")
      if function == "add":
        value += " "+raw_input("Value (cannot contain ':' or ' '): ")
    except (EOFError):
      break
    message = ":".join([str(MessageId),value])
    sending = True
    while(sending):
      data = send_value(LEADER, message, requestTimeout)
      if data == errno.ETIMEDOUT:
        time.sleep(.25)
        LEADER = random.sample(set(NAMES.values())-set([LEADER]),1)[0]
      elif len(data) > 1:
        if data[0] == 'R':
          data = data.split(":")
          chosen_value = data[1]
          response = data[2].split(",")
          if chosen_value == value:
            function = chosen_value.split(" ")[0]
            if function == "add":
              print ("Added Value: %s at Key: %s")%(response[1], response[0])
            elif function == "get":
              print response
              if len(response) > 1:
                print ("Value at Key: %s is %s")%(response[0], response[1])
              else:
                print ("Invalid Key:%s")%(response[0])
            elif function == "remove":
              print "Removed value at Key: %s"%response[0]
            write_log("Chosen,%d,%s"%(MessageId, chosen_value))
            CHOSEN = max(CHOSEN, MessageId)
            sending = False
          else:
            MessageId += 1
            write_log("Chosen,%d,%s"%(MessageId, chosen_value))
            message = ":".join([str(MessageId),value])
        else:
          LEADER = data
    MessageId += 1

def exit_handler(signal = None, frame = None):
  print ""
  sys.exit(0)

if __name__ == "__main__":
  signal.signal(signal.SIGINT, exit_handler)
  atexit.register(exit_handler)
  settings = load_settings(["../settings/settings.csv","../settings/client_settings.csv","log.csv"])
  get_server_info(settings["numNodes"])

  id = 0
  if "Chosen" in settings.keys():
    id = int(settings["Chosen"])
  process_input(id, int(settings["requestTimeout"]))
