from communication import *
from heartbeat import *
from message import *
import time, csv

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

if __name__ == "__main__":
  setup()
  while(1):
    heartbeat_send()
    message_check()
    local_message_check()



