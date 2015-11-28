import communication, time, message

# Time since last heartbeat for each node
BEAT_TIME = {}
LAST_BEAT = None
BEAT_DELAY = None
BEAT_TIMEOUT = None

def heartbeat_send():
  global LAST_BEAT
  if (time.time()-LAST_BEAT) > BEAT_DELAY:
    communication.send_mess(message.Message(Message = "Heartbeat"))
    LAST_BEAT = time.time()

def heartbeat_process(name, message):
  global BEAT_TIME
  BEAT_TIME[name] = time.time()

def heartbeat_check(name):
  t = time.time()
  if t-BEAT_TIME[name]>BEAT_TIMEOUT:
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
