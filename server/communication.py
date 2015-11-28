from threading import *
from message import *
from heartbeat import *
from propose import *
import socket, threading, select, sys, errno, csv, time, atexit, math

OUT_BUF = []
OUT_BUF_LOCK = threading.Lock()

IN_BUF = []
IN_BUF_LOCK = threading.Lock()

# IP to name mapping
NAMES = {}
# Name to (IP,Port) mapping
ADDRESSES = {}
# Name to Node ID Mapping
NODE_ID = {}
# Name to status mapping (0=offline, 1=online,2=leader)
STATUS = {}
LEADER = None
ROUND = 0

LISTEN = None

HOSTNAME = None

MAJORITY = 0

def exit_handler():
  if LISTEN:
    LISTEN.close()

def connection_init(num_nodes):
  global NAMES, ADDRESSES, STATUS, NODE_ID, SOCKETS, LISTEN, LEADER, HOSTNAME, MAJORITY
  atexit.register(exit_handler)

  HOSTNAME = socket.gethostname().split(".")[0]
  MAJORITY = num_nodes/2+1

  with open('../settings/peernames%d.csv'%num_nodes, 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
      ADDRESSES[row[0]] = (row[1], int(row[2]))
      if(row[0] != HOSTNAME):
        STATUS[row[0]] = 0
        NAMES[row[1]] = row[0]
      NODE_ID[row[0]] = int(row[3])

  leader_id = max(NODE_ID.values())
  for name,nid in NODE_ID.items():
    if nid == leader_id:
      LEADER = name
      ROUND = 1
      setProposalId("%d.%d"%(ROUND, NODE_ID[LEADER]))

  print "[NODE %d: %s]" % (NODE_ID[HOSTNAME],HOSTNAME)

  time.sleep(1)
  print "Initiating Connections on port: %d" % ADDRESSES[HOSTNAME][1]

  try:
    LISTEN = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    LISTEN.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    LISTEN.setblocking(0)
    sock_address = (socket.gethostbyname(socket.gethostname()) , ADDRESSES[HOSTNAME][1])
    LISTEN.bind(sock_address)
    LISTEN.listen(20)
  except socket.error, e:
    print e
    sys.exit()

  for name in NAMES.values():
    while(1):
      try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(ADDRESSES[name])
        s.close()
        break
      except socket.error, e:
        err = e.args[0]
        if err != errno.ECONNREFUSED and err!=errno.ETIMEDOUT:
          print e
          sys.exit(1)
  last_t = 0
  while(0 in STATUS.values()):
    if last_t==0 or (time.time()-last_t > 1):
      for name, status in STATUS.items():
        if status == 0:
          last_t = time.time()
          try:
            print "Trying to connect with %s"% name
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(ADDRESSES[name])
            s.send("?")
            s.close()
          except socket.error, e:
            err = e.args[0]
            if err != errno.EAGAIN and err != errno.ETIMEDOUT and errno.ECONNREFUSED:
              print e
              sys.exit(1)
    try:
      c, address = LISTEN.accept()
      data = c.recv(4096)
      c.close()
      name = NAMES[address[0]]
      if data == "?":
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(ADDRESSES[name])
        s.send("ACK")
        s.close()
      if data == "ACK":
        if NAMES[address[0]] == LEADER:
          STATUS[name] = 2
          print "Connected with[NODE %d: %s]: Status 2 (Leader)"% (NODE_ID[name],name)
        else:
          STATUS[name] = 1
          print "Connected with[NODE %d: %s]: Status 1"% (NODE_ID[name],name)
      if data == "RESET":
        print "Darn!!!"
        sys.exit(1)
    except socket.error, e:
      err = e.args[0]
      if err != errno.EAGAIN and err != errno.EWOULDBLOCK and err != errno.ETIMEDOUT:
        print e
        sys.exit(1)

  comm_thread = Thread(target = connection_thread)
  comm_thread.daemon = True
  comm_thread.start()

def connection_thread():
  last_beat_check = time.time()

  while (1):
    try:
      c, address = LISTEN.accept()
      data = c.recv(4096)
      c.close()
      ip = address[0]
      if ip in NAMES.keys():
        in_mess = Message(Addr = NAMES[address[0]], Message = data)
      else:
        in_mess = Message(Addr = address, Message = data, Client=True)
      IN_BUF_LOCK.acquire()
      IN_BUF.append(in_mess)
      IN_BUF_LOCK.release()
    except socket.error, e:
      err = e.args[0]
      if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
        print e
        sys.exit(1)
    OUT_BUF_LOCK.acquire()
    if len(OUT_BUF):
      out_mess = OUT_BUF.pop(0)
      OUT_BUF_LOCK.release()
      out_addr = out_mess.get_addr()
      if not out_mess.is_client():
        out_addr = ADDRESSES[out_addr]
      else:
        out_addr = (out_addr[0], 10010)
      try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(out_addr)
        s.send(out_mess.get_mess())
        s.close()
      except socket.error, e:
        err = e.args[0]
        if err != errno.ECONNREFUSED and err != errno.ETIMEDOUT and err != errno.EADDRNOTAVAIL:
          print str(out_addr) + ":"+out_mess.get_mess()
          print e
          sys.exit(1)
        elif out_mess.get_wait():
          OUT_BUF_LOCK.acquire()
          OUT_BUF.append(out_mess)
          OUT_BUF_LOCK.release()
    else:
      OUT_BUF_LOCK.release()

    if (time.time()-last_beat_check)>1:
      for name in STATUS.keys():
        if STATUS[name]:
          if not heartbeat_check(name):
            STATUS[name] = 0
            print "STATUS of [NODE %d: %s] has changed to 0" % (NODE_ID[name], name)

def send_mess(message):
  OUT_BUF_LOCK.acquire()
  if not message.get_addr():
    mess = message.get_mess()
    for name in NAMES.values():
      if STATUS[name] != 0:
        OUT_BUF.append(Message(Addr = name, Message = mess))
  else:
    OUT_BUF.append(message)
  OUT_BUF_LOCK.release()

def recv_mess():
  message = None
  IN_BUF_LOCK.acquire()
  if len(IN_BUF):
    message = IN_BUF.pop(0)
    IN_BUF_LOCK.release()
  else:
    IN_BUF_LOCK.release()
  return message

def get_names():
  return NAMES.values()

def am_leader():
  HOSTNAME = socket.gethostname().split(".")[0]
  if HOSTNAME == LEADER:
    return True
