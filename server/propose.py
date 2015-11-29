import communication, message

ClientsAddr = {}
ClientsMessageById = {}

P_Proposals = {}
P_Values = {}
NumAccepted = {}
P_Accepted = {}
NumPrepared = {}
Chosen = {}

firstUnchosenIndex = 0
EmptyIndex = 0

ProposalId = None

def client_accept(index):
  addr = ClientsAddr[index]
  if addr:
    value = P_Values[index]
    client_reply_mess = "R:%s:%d:%s"%(ProposalId, index,value)
    client_reply = message.Message(Addr = addr, Wait = True, Client = True,
      Message = client_reply_mess)
    communication.send_mess(client_reply)

def propose(mess):
  global EmptyIndex, firstUnchosenIndex

  client_addr = mess.get_addr()
  ClientsAddr[EmptyIndex] = client_addr
  data = mess.get_mess()
  [message_id, value] = data.split(":")

  if client_addr:
    clientip = client_addr[0].split(".")[-1]
    message_id = "%s.%s"%(clientip,message_id)
    ClientsMessageById[message_id] = value

  P_Proposals[EmptyIndex] = ProposalId
  P_Values[EmptyIndex] = value
  NumPrepared[EmptyIndex] = 0
  P_Accepted[EmptyIndex] = False
  NumAccepted[EmptyIndex] = 0
  Chosen[EmptyIndex] = False

  proposal = "P:%s:%s:%s:%d:%d"%(ProposalId, value, message_id,
    EmptyIndex, firstUnchosenIndex)
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
  global firstUnchosenIndex, P_Proposals, NumPrepared, P_Values, Chosen
  [index, prepared_ProposalId, prepared_value] = args
  index = int(index)

  if not P_Accepted[index]:

    NumPrepared[index] += 1
    if float(P_Proposals[index]) < float(prepared_ProposalId):
      P_Proposals[index] = prepared_ProposalId
      P_Values[index] = prepared_value
    if NumPrepared[index] == communication.MAJORITY and P_Values[index] != "PI":
      P_Accepted[index] = True

      response = "A:%d:%s:%s"%(index, P_Proposals[index], P_Values[index])
      response_mess = message.Message(Wait = True, Message = response)
      communication.send_mess(response_mess)
      local_response_mess = message.Message(Local = True, Message = response)
      communication.send_local_mess(local_response_mess)
def success_response(acceptors_firstUnchosen):
  if acceptors_firstUnchosen < firstUnchosenIndex:
    i = acceptors_firstUnchosen
    response = "%d:%s:%s"%(i,P_Proposals[i], P_Values[i])
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
    if P_Proposals[index] != proposalId:
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
