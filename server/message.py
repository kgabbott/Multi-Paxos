import communication, heartbeat, local_communication, propose, accept
class Message(object):

  def __init__(self,Addr = None, Local = False, Message = "", Wait = False, Client = False):
    self.Addr = Addr
    self.Local = Local
    self.Message = Message
    self.Wait = Wait
    self.Client = Client

  def get_addr(self):
    return self.Addr

  def get_local(self):
    return self.Local

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
      propose.propose(message)
    else:
      communication.send_mess(Message(Addr = name, Wait = True, Client = True,
        Message = "%s" % communication.LEADER))
  elif data == "Heartbeat":
    heartbeat.heartbeat_process(name, data)
  elif data == "?":
    communication.send_mess(Message(Addr = addr, Message = "RESET"))
    print "[NODE %d: %s] is back online" % (NODE_ID[addr], addr)
  elif len(data)>1:
    print ("[%s] %s") %(name, data)
    data_split = data.split(":")
    function = data_split[0]
    response = None
    if function == "P":
      response = accept.propose_process(data_split[1:])
      response = "PR:"+response
      communication.send_mess(Message(Addr = name, Wait = True, Message = response))

    elif function == "PR":
      propose.prepare_response(data_split[1:])

    elif function == "A":
      response = accept.accept_response(data_split[1:])
      response = "AR:"+response
      communication.send_mess(Message(Addr = name, Wait = True, Message = response))

    elif function == "AR":
      response = propose.accept_response(data_split[1:])
      if response:
        response = "S:"+response
        communication.send_mess(Message(Addr = name, Wait = True, Message = response))

    elif function == "S":
      response = accept.success(data_split[1:])
      if response:
        response = "SR:"+response
        communication.send_mess(Message(Addr = name, Wait = True, Message = response))

    elif function == "SR":
      response = propose.success_response(int(data[1]))
      if response:
        response = "S:"+response
        communication.send_mess(Message(Addr = name, Wait = True, Message = response))

def  message_check():
  message = communication.recv_mess()
  if message:
    message_process(message)

def local_message_process(message):
  data = message.get_mess()
  print ("[%s] %s") %("Local", data)
  data_split = data.split(":")
  function = data_split[0]
  response = None
  if function == "P":
    response = accept.propose_process(data_split[1:])
    response = "PR:"+response
    local_response = Message(Local = True, Message = response)
    local_communication.send_local_mess(local_response)

  elif function == "PR":
    propose.prepare_response(data_split[1:])

  elif function == "A":
    response = accept.accept_response(data_split[1:])
    response = "AR:"+response
    local_response = Message(Local = True, Message = response)
    local_communication.send_local_mess(local_response)

  elif function == "AR":
    reponse = propose.accept_response(data_split[1:])
    if response:
      response = "S:"+response
      local_response = Message(Local = True, Message = response)
      local_communication.send_local_mess(local_response)

  elif function == "S":
    response = accept.success(data_split[1:])
    if response:
      response = "SR:"+response
      local_response = Message(Local = True, Message = response)
      local_communication.send_local_mess(local_response)

  elif function == "SR":
    response = propose.success_response(int(data[1]))
    if response:
      response = "S:"+response
      local_response = Message(Local = True, Message = response)
      local_communication.send_local_mess(local_response)
def local_message_check():
  message = local_communication.get_local_mess()
  if message:
    local_message_process(message)

def loop():
  print "hell0"
