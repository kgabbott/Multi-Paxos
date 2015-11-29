import communication, message

ClientsAddr = {}
ClientsId = {}

P_Proposals = {}
P_Values = {}
NumAccepted = {}
P_Accepted = {}
NumPrepared = {}
Chosen = {}

firstUnchosenIndex = 0
EmptyIndex = 0

ProposalId = None

def client_respond(addr, index):
  if addr:
    value = P_Values[index]
    client_reply_mess = "R:%s"%(value)
    client_reply = message.Message(Addr = addr, Wait = True, Client = True,
      Message = client_reply_mess)
    communication.send_mess(client_reply)

def propose(mess):
  global ClientsAddr, ClientsId
  global P_Proposals, P_Values, NumAccepted, P_Accepted, NumPrepared, Chosen
  global firstUnchosenIndex, EmptyIndex

  client_addr = mess.get_addr()
  data = mess.get_mess()
  [message_id, value] = data.split(":")

  if client_addr:
    clientip = client_addr[0].split(".")[-1]
    client_id = "%s.%s"%(clientip,message_id)
  else:
    client_id = "P"

  next_index = EmptyIndex
  if client_id != "P":
    for index, cid in ClientsId.items():
      if cid == client_id:
        if Chosen[index]:
          client_respond(client_addr, index)
          return
        else:
          next_index = index
          break

  ClientsAddr[next_index] = client_addr
  P_Proposals[next_index] = ProposalId
  P_Values[next_index] = value
  ClientsId[next_index] = client_id
  NumPrepared[next_index] = 0
  P_Accepted[next_index] = False
  NumAccepted[next_index] = 0
  Chosen[next_index] = False

  proposal = "P:%s:%s:%s:%d:%d"%(ProposalId, value, client_id,
    next_index, firstUnchosenIndex)
  proposal_mess = message.Message(Wait = True, Message = proposal)
  communication.send_mess(proposal_mess)
  local_proposal_mess = message.Message(Local = True, Message = proposal)
  communication.send_local_mess(local_proposal_mess)

  EmptyIndex+=1

def propose_init(maxIndex):
  global EmptyIndex, ProposalId
  ProposalId = "0.0"
  for i in range(0, maxIndex+1):
    propose(message.Message(Message="0:PI"))
  EmptyIndex = maxIndex+1
  ProposalId = communication.get_proposal_id()


def prepare_response(args):
  global ClientsAddr, ClientsId
  global P_Proposals, P_Values, NumAccepted, P_Accepted, NumPrepared
  [index, prepared_ProposalId, prepared_clientid, prepared_value] = args
  index = int(index)

  if not P_Accepted[index]:
    NumPrepared[index] += 1
    if float(P_Proposals[index]) < float(prepared_ProposalId):
      if ClientsId[index] != "P":
        propose(message.Message(Addr = ClientsAddr[index],
          Message="%s:%s"%(ClientsId[index].split(".")[1], P_Proposals[index])))
        del ClientsAddr[index]
      P_Proposals[index] = prepared_ProposalId
      P_Values[index] = prepared_value
      ClientsId[index] = prepared_clientid

    if NumPrepared[index] == communication.MAJORITY and P_Values[index] != "PI":
      P_Accepted[index] = True

      response = "A:%d:%s:%s:%s"%(index, P_Proposals[index],  ClientsId[index], P_Values[index])
      response_mess = message.Message(Wait = True, Message = response)
      communication.send_mess(response_mess)
      local_response_mess = message.Message(Local = True, Message = response)
      communication.send_local_mess(local_response_mess)

def success_response(acceptors_firstUnchosen):
  if acceptors_firstUnchosen < firstUnchosenIndex:
    i = acceptors_firstUnchosen
    response = "%d:%s:%s:%s"%(i, P_Proposals[i], ClientsId[i], P_Values[i])
  else:
    response = None
  return response

def accept_response(args):
  global ClientsAddr, ClientsId, Chosen
  global P_Proposals, P_Values, NumAccepted, P_Accepted, NumPrepared, Chosen
  global firstUnchosenIndex, EmptyIndex

  [index, proposalId,  clientid, value, acceptors_firstUnchosen] = args
  index = int(index)
  acceptors_firstUnchosen = int(acceptors_firstUnchosen)

  if not Chosen[index]:
    NumAccepted[index] += 1
    if P_Proposals[index] != proposalId:
      NumPrepared[index] = -1*communication.MAJORITY
      if ClientsId[index] != "P":
        propose(message.Message(Addr = ClientsAddr[index],
          Message="%s:%s"%(ClientsId[index].split(".")[1], P_Proposals[index])))
        del ClientsAddr[index]
    if NumPrepared[index] == communication.MAJORITY:
      Chosen[index] = True
      print "[Proposer] The value [%s] at index:%d has been accepted"%(value, index)
      client_respond(ClientsAddr[index], index)

      if index == firstUnchosenIndex:
        while(firstUnchosenIndex<EmptyIndex):
          if firstUnchosenIndex in Chosen.keys() and Chosen[firstUnchosenIndex]:
            firstUnchosenIndex+=1
          else:
            break
  return success_response(acceptors_firstUnchosen)
