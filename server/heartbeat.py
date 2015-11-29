import communication, time, message, accept, propose

# Time since last heartbeat for each node
BEAT_TIME = {}
LAST_BEAT = None
BEAT_DELAY = None
BEAT_TIMEOUT = None

HEARTBEAT_MESSAGES = {}

def heartbeat_send():
  global LAST_BEAT
  if (time.time()-LAST_BEAT) > BEAT_DELAY:
    lead = communication.LEADER
    if not lead or not communication.STATUS[lead]:
      communication.choose_new_leader()
      lead = communication.NEW_LEADER
    heartbeat_mess = "H:%s:%d:%d"%(lead, communication.ROUND, accept.get_largest_index())
    communication.send_mess(message.Message(Message = heartbeat_mess))
    communication.send_local_mess(message.Message(Local = True, Message = heartbeat_mess))
    LAST_BEAT = time.time()

def new_leader_check():
  global HEARTBEAT_MESSAGES
  messages = HEARTBEAT_MESSAGES.values()
  leaders = [message[0] for message in messages]
  for other_leader in set(leaders):
    if leaders.count(other_leader) >= communication.MAJORITY:
      if other_leader and other_leader != communication.LEADER:
        rounds = [int(message[1]) for message in messages]
        new_round = max(rounds)
        if communication.LEADER:
          new_round += 1
        indices = [int(message[2]) for message in messages]
        max_index = max(indices)
        communication.new_leader(other_leader, new_round, max_index)
        HEARTBEAT_MESSAGES = {}

def heartbeat_process(name, message):
  global BEAT_TIME, HEARTBEAT_MESSAGES
  BEAT_TIME[name] = time.time()
  HEARTBEAT_MESSAGES[name] = message
  new_leader_check()

def heartbeat_check(name):
  t = time.time()
  if t-BEAT_TIME[name]>BEAT_TIMEOUT:
    if name in HEARTBEAT_MESSAGES.keys():
      del HEARTBEAT_MESSAGES[name]
    return False
  return True

def heartbeat_init(beatDelay, beatTimeout):
  global BEAT_TIME, LAST_BEAT, BEAT_TIMEOUT, BEAT_DELAY
  names = communication.get_names()
  for name in names:
    BEAT_TIME[name] = time.time()
  LAST_BEAT = time.time()
  BEAT_DELAY = beatDelay
  BEAT_TIMEOUT = beatTimeout

def reset(name):
  BEAT_TIME[name] = time.time()

