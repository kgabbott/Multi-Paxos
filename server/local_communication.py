import threading

LOCAL_MESS_BUF = []
LOCAL_LOCK = threading.Lock()

def send_local_mess(mess):
  LOCAL_LOCK.acquire()
  LOCAL_MESS_BUF.append(mess)
  LOCAL_LOCK.release()

def get_local_mess():
  LOCAL_LOCK.acquire()
  mess = None
  if len(LOCAL_MESS_BUF):
    mess = LOCAL_MESS_BUF.pop(0)
  LOCAL_LOCK.release()
  return mess
