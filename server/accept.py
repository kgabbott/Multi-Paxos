import communication, message, csv, os

A_Values = {}
A_Proposals = {}
A_Accepted = {}

ClientsId = {}

firstUnchosenIndex = 0
largestIndex = -1

def accept_value(index):
  with open("storage/storage_%s.csv"%communication.HOSTNAME,"a+") as f:
    f.write("%d,%s,%s,%s\n"%(index, A_Proposals[index], ClientsId[index], A_Values[index]))

def accept_init():
  global A_Values, A_Proposals, A_Accepted, firstUnchosenIndex, largestIndex
  storage_file = "storage/storage_%s.csv"%communication.HOSTNAME
  if os.path.exists(storage_file):
    with open(storage_file, 'rb') as f:
      reader = csv.reader(f)
      for row in reader:
        [i, proposal, client_id, value] = row
        i = int(i)
        A_Values[i] = value
        A_Proposals[i] = proposal
        ClientsId[i] = client_id
        A_Accepted[i] = 1
        print "Loaded accepted value: [%s] at index: %d" % (value, i)
        if i > largestIndex:
          largestIndex = i
  minUnchosen = largestIndex
  for i in range(0, largestIndex):
    if i not in A_Accepted.keys():
      A_Accepted[i] = 0
      if minUnchosen == largestIndex:
        minUnchosen = i
  firstUnchosenIndex = max(0,minUnchosen)

def propose_process(args):
  global firstUnchosenIndex, A_Proposals, A_Values, A_Accepted, largestIndex

  [ProposalId, value, client_id, index, proposers_firstunchosen] = args
  index = int(index)
  proposers_firstunchosen = int(proposers_firstunchosen)

  if index in A_Values.keys():
    if not A_Accepted[index] and float(A_Proposals[index]) < float(ProposalId):
      A_Proposals[index] = ProposalId
      A_Values[index] = value
      ClientsId[index] = client_id
  else:
    A_Values[index] = value
    A_Accepted[index] = 0
    A_Proposals[index] = ProposalId
    ClientsId[index] = client_id

  i = proposers_firstunchosen-1
  while i >= firstUnchosenIndex:
    if i in A_Accepted.keys():
      if not A_Accepted[i]:
        if A_Proposals[i] == ProposalId:
          A_Proposals[i] == "inf"
          A_Accepted[i] = 1
          accept_value(i)
        else:
          firstUnchosenIndex = i
    i -= 1

  largestIndex = max(index, largestIndex)

  response = "%d:%s:%s:%s"%(index, A_Proposals[index], ClientsId[index], A_Values[index])
  return response

def accept_response(args):
  global A_Accepted, A_Proposals, firstUnchosenIndex
  [index, ProposalId, client_id, value] = args
  index = int(index)

  newlyAccepted = False
  if not A_Accepted[index]:
    newlyAccepted = True

  if index not in A_Proposals.keys():
    A_Proposals[index] = "-inf"

  if float(A_Proposals[index]) <= float(ProposalId):
    A_Accepted[index] = 1
    if float(A_Proposals[index]) < float(ProposalId):
      A_Proposals[index] = ProposalId
      A_Values[index] = value
      ClientsId[index] = index
      newlyAccepted = True
    if newlyAccepted:
      accept_value(index)
      print "[Acceptor] The value [%s] at index:%d has been chosen"%(A_Values[index], index)

    if index == firstUnchosenIndex:
      while(firstUnchosenIndex<largestIndex+1):
        if firstUnchosenIndex in A_Accepted.keys() and A_Accepted[firstUnchosenIndex]:
          firstUnchosenIndex+=1
        else:
          break

  response = ("%d:%s:%s:%s:%d")%(index, A_Proposals[index], ClientsId[index], A_Values[index], firstUnchosenIndex)
  return response

def success(args):
  global firstUnchosenIndex, A_Accepted, A_Proposals, A_Values
  [index, ProposalId, ClientsId, value] = args
  index = int(index)

  if index not in A_Accepted.keys():
    A_Accepted[index] = 0

  if not A_Accepted[index]:
    A_Accepted[index] = True
    A_Proposals[index] = ProposalId
    A_Values[index] = value
    ClientsId[index] = ClientsId
    print "[Acceptor] The value [%s] at index:%d has been chosen"%(A_Values[index], index)
    accept_value(index)

    if index == firstUnchosenIndex:
      while(firstUnchosenIndex<largestIndex+1):
        if firstUnchosenIndex in A_Accepted.keys() and A_Accepted[firstUnchosenIndex]:
          firstUnchosenIndex+=1
        else:
          break
  if firstUnchosenIndex<largestIndex:
    response = ("%d")%(firstUnchosenIndex)
  else:
    response = None
  return response

def get_largest_index():
  return largestIndex
