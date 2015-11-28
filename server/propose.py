import communication, message, local_communication

ClientsAddr = {}
ClientsMessageById = {}

Proposals = {}
Values = {}
NumAccepted = {}
Accepted = {}
NumPrepared = {}
Chosen = {}

firstUnchosenIndex = 0
EmptyIndex = 0

ProposalId = None

def client_accept(index):
  addr = ClientsAddr[index]
  value = Values[index]
  client_reply_mess = "R:%s:%d:%s"%(ProposalId, index,value)
  client_reply = message.Message(Addr = addr, Wait = True, Client = True,
    Message = client_reply_mess)
  communication.send_mess(client_reply)

def propose(mess):
  global EmptyIndex, firstUnchosenIndex

  client_addr = mess.get_addr()
  ClientsAddr[EmptyIndex] = client_addr
  clientip = client_addr[0].split(".")[-1]

  data = mess.get_mess()
  [message_id, value] = data.split(":")
  message_id = "%s.%s"%(clientip,message_id)

  ClientsMessageById[message_id] = value

  Proposals[EmptyIndex] = ProposalId
  Values[EmptyIndex] = value
  NumPrepared[EmptyIndex] = 0
  Accepted[EmptyIndex] = False
  NumAccepted[EmptyIndex] = 0
  Chosen[EmptyIndex] = False

  proposal = "P:%s:%s:%s:%d:%d"%(ProposalId, value, message_id,
    EmptyIndex, firstUnchosenIndex)
  proposal_mess = message.Message(Wait = True, Message = proposal)
  communication.send_mess(proposal_mess)
  local_proposal_mess = message.Message(Local = True, Message = proposal)
  local_communication.send_local_mess(local_proposal_mess)

  EmptyIndex+=1

def prepare_response(args):
  global firstUnchosenIndex, Proposals, NumPrepared, Values, Chosen
  [index, prepared_ProposalId, prepared_value] = args
  index = int(index)

  if not Accepted[index]:
    NumPrepared[index] += 1
    if float(Proposals[index]) < float(prepared_ProposalId):
      Proposals[index] = prepared_ProposalId
      Values[index] = prepared_value
    if NumPrepared[index] == communication.MAJORITY:
      Accepted[index] = True

      response = "A:%d:%s:%s"%(index, Proposals[index], Values[index])
      response_mess = message.Message(Wait = True, Message = response)
      communication.send_mess(response_mess)
      local_response_mess = message.Message(Local = True, Message = response)
      local_communication.send_local_mess(local_response_mess)

def success_response(acceptors_firstUnchosen):
  if acceptors_firstUnchosen < firstUnchosenIndex:
    i = acceptors_firstUnchosen
    response = "%d:%s:%s"%(i,Proposals[i], Values[i])
  else:
    response = None
  return response

def accept_response(args):
  global NumAccepted, Chosen, firstUnchosenIndex

  [index, proposalId, value, acceptors_firstUnchosen] = args
  index = int(index)
  acceptors_firstUnchosen = int(acceptors_firstUnchosen)

  if not Chosen[index]:
    NumAccepted[index] += 1
    if Proposals[index] != proposalId:
      NumPrepared[index] = -1*communication.MAJORITY
      # Restart
    if NumPrepared[index] == communication.MAJORITY:
      Chosen[index] = True
      print "[Proposer] The value [%s] at index:%d has been accepted"%(value, index)
      client_accept(index)

      if index == firstUnchosenIndex:
          while(firstUnchosenIndex<EmptyIndex):
            if Chosen[firstUnchosenIndex]:
              firstUnchosenIndex+=1
  return success_response(acceptors_firstUnchosen)

def setProposalId(id):
  global ProposalId
  ProposalId = id
