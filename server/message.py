import communication, heartbeat, local_communication, propose, accept
class Message(object):

  def __init__(self,Addr = None, Local_Addr = None, Message = "", Wait = False, Client = False):
    self.Addr = Addr
    self.Local_Addr = Local_Addr
    self.Message = Message
    self.Wait = Wait
    self.Client = Client

  def get_addr(self):
    return self.Addr

  def get_local(self):
    return self.Local_Addr

  def get_mess(self):
    return self.Message

  def get_wait(self):
    return self.Wait

  def is_client(self):
    return self.Client

def message_process(message):
  name = message.get_addr()
  data = message.get_mess()
  if message.is_client():
    if communication.am_leader():
      communication.send_mess(Message(Addr = name, Wait = True, Client = True,
        Message = "Processing"))
    else:
      communication.send_mess(Message(Addr = name, Wait = True, Client = True,
        Message = "%s" % communication.LEADER))
  elif data == "Heartbeat":
    heartbeat.heartbeat_process(name, data)
  elif data == "?":
    communication.send_mess(Message(Addr = addr, Message = "RESET"))
    print "[NODE %d: %s] is back online" % (NODE_ID[addr], addr)

def  message_check():
  message = communication.recv_mess()
  if message:
    message_process(message)

def local_message_process(message):
  function = message.get_local()
  data = message.get_mess()

def local_message_check():
  message = local_communication.get_local_mess()
  if message:
    local_message_process(message)
