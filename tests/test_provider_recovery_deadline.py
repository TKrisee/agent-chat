import unittest
from unittest.mock import patch
from agent_chat import provider_recovery_wire as wire
class Deadline(unittest.TestCase):
 def test_native_request_uses_remaining_operation_budget_not_three_second_cap(self):
  connection=object.__new__(wire.Wire);connection.deadline=120
  with patch.object(wire.time,'monotonic',return_value=5):self.assertEqual(connection.remaining(),115)
 def test_partial_frame_reads_do_not_reset_absolute_deadline(self):
  class Socket:
   def __init__(self):self.reads=0;self.timeouts=[]
   def settimeout(self,value):self.timeouts.append(value)
   def recv(self,count):self.reads+=1;return b'x'
  sock=Socket()
  with patch.object(wire.time,'monotonic',side_effect=[10,16]):
   with self.assertRaises(TimeoutError):wire.exact(sock,2,15)
  self.assertEqual(sock.reads,1);self.assertEqual(sock.timeouts,[5])

class RealDeadline(unittest.TestCase):
 def response(self,delay,budget):
  import socket,threading,time
  left,right=socket.socketpair();calls=[]
  def server():
   try:
    calls.append(wire.receive_frame(right));threading.Event().wait(delay)
    wire.send_frame(right,'{"id":1,"result":{"ok":true}}')
   except OSError:pass
   finally:right.close()
  runner=threading.Thread(target=server);runner.start()
  client=object.__new__(wire.Wire);client.sock=left;client.serial=0;client.deadline=time.monotonic()+budget
  before=time.monotonic()
  try:return client.request('private/read','{}'),time.monotonic()-before,calls
  finally:left.close();runner.join(timeout=delay+1)
 def test_real_response_after_three_seconds_within_original_budget(self):
  raw,elapsed,calls=self.response(3.2,5)
  self.assertEqual(wire.field(raw,'.ok|tostring'),'true');self.assertGreaterEqual(elapsed,3);self.assertLess(elapsed,5);self.assertEqual(len(calls),1)
 def test_real_original_deadline_expires_without_request_replay(self):
  import time
  before=time.monotonic()
  with self.assertRaises(TimeoutError):self.response(.15,.05)
  self.assertLess(time.monotonic()-before,.5)
