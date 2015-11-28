import communication, message, local_communication

Values = {}
Proposals = {}
Accepted = {}

ClientsMessageById = {}

firstUnchosenIndex = 0
largestIndex = 0

def propose_process(args):
  global firstUnchosenIndex, Proposals, Values, Accepted, largestIndex

  [ProposalId, value, message_id, index, proposers_firstunchosen] = args
  index = int(index)
  proposers_firstunchosen = int(proposers_firstunchosen)

  ClientsMessageById[message_id] = value
  if index in Values.keys():
    if not Accepted[index] and float(Proposals[index]) < float(ProposalId):
      Proposals[index] = ProposalId
      Values[index] = value
  else:
    Values[index] = value
    Accepted[index] = 0
    Proposals[index] = ProposalId

  i = proposers_firstunchosen-1
  while i >= firstUnchosenIndex:
    if i in Accepted.keys():
      if not Accepted[i]:
        if Proposals[i] == ProposalId:
          Proposals[i] == "inf"
          Accepted[i] = 1
        else:
          firstUnchosenIndex = i
    i -= 1

  largestIndex = max(index, largestIndex)

  response = "%d:%s:%s"%(index, Proposals[index], Values[index])
  return response

def accept_response(args):
  global Accepted, firstUnchosenIndex
  [index, ProposalId, value] = args
  index = int(index)
  if float(Proposals[index]) <= float(ProposalId):
    Accepted[index] = 1
    if float(Proposals[index]) < float(ProposalId):
      Proposals[index] = ProposalId
      Values[index] = value

    print "[Acceptor] The value [%s] at index:%d has been chosen"%(Values[index], index)

    if index == firstUnchosenIndex:
      while(firstUnchosenIndex<largestIndex+1):
          if Accepted[firstUnchosenIndex]:
            firstUnchosenIndex+=1

  response = ("%d:%s:%s:%d")%(index, Proposals[index], Values[index], firstUnchosenIndex)
  return response

def success(args):
  global firstUnchosenIndex, Accepted, Proposals, Values
  [index, ProposalId, value] = args
  index = int(index)
  if not Accepted[index]:
    Accepted[index] = True
    Proposals[index] = ProposalId
    Values[index] = value

    if index == firstUnchosenIndex:
      while(firstUnchosenIndex<largestIndex+1):
          if Accepted[firstUnchosenIndex]:
            firstUnchosenIndex+=1
  if firstUnchosenIndex<largestIndex:
    response = ("%d")%(firstUnchosenIndex)
  else:
    response = None
  return response





