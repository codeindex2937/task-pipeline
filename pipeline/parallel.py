import threading
import traceback
import time


SIGNAL_STOP = {}

class SimpleThread(threading.Thread):
	def __init__(self):
		super(SimpleThread, self).__init__()
		self.running = False

	def abort(self):
		self.running = False

	def run(self):
		self.running = True
		self.on_start()

		self.exec()

		self.on_abort()

	def exec(self):
		pass
	def on_start(self):
		pass
	def on_abort(self):
		pass

class SimpleWorker(SimpleThread):
	def __init__(self, pipe, config):
		super(SimpleWorker, self).__init__()
		self.outputs = []
		self.input = pipe
		self.config = config
		self.idle_fallback = 1

	def send_to(self, pipe):
		self.outputs.append(pipe)

	def output(self, item):
		for pipe in self.outputs:
			pipe.append(item)

	def exec(self):
		while self.running:
			try:
				if len(self.input) == 0:
					time.sleep(self.idle_fallback)
					self.idle_fallback = min(self.idle_fallback + 1, 10)
				else:
					data = self.input.popleft()
					if data is SIGNAL_STOP:
						self.abort()
					else:
						self.process(data)
						self.idle_fallback = 1
			except:
				traceback.print_exc()
