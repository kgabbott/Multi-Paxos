from communication import *
from heartbeat import *
from message import *
import time, csv, accept
import signal, sys

def load_settings(setting_files):
  settings = {}
  for s in setting_files:
    with open(s, 'rb') as f:
      reader = csv.reader(f)
      for row in reader:
        settings[row[0]] = row[1]
  return settings

def setup():
  settings = load_settings(["../settings/settings.csv","../settings/server_settings.csv"])
  connection_init(int(settings["numNodes"]))
  heartbeat_init(float(settings["beatDelay"]), int(settings["beatTimeout"]))
  accept.accept_init()

def signal_handler(signal, frame):
  print ""
  sys.exit(0)

if __name__ == "__main__":
  signal.signal(signal.SIGINT, signal_handler)
  setup()
  while(1):
    heartbeat_send()
    message_check()
    local_message_check()
