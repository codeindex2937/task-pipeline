from .parallel import SimpleThread

import threading
import queue
import traceback


class ServiceManager:
	port_map = dict()
	@classmethod
	def register(cls, port_type, name):
		client_port = queue.Queue()
		server_port = queue.Queue()
		cls.port_map[name] = port_type(client_port, server_port)
		return (client_port, server_port)

	@classmethod
	def get(cls, name):
		return cls.port_map[name]

class ServiceWorker(SimpleThread):
	def __init__(self, config, pipe_out, pipe_in):
		super(ServiceWorker, self).__init__()
		self.config = config
		self.pipe_out = pipe_out
		self.pipe_in = pipe_in

	def exec(self):
		while self.running:
			result = {}
			try:
				req = self.pipe_in.get(timeout=1)
				try:
					result = self.process(req['data'])
				except:
					traceback.print_exc()
				if not req['async']:
					self.pipe_out.put(result)
			except queue.Empty:
				pass

	class Port:
		def __init__(self, service_output, service_port):
			self.service_port = service_port
			self.service_output = service_output
			self.lock = threading.Lock()

		def request(self, data, no_wait=True):
			with self.lock:
				self.service_port.put({'async': no_wait, 'data': data})
				if not no_wait:
					return self.service_output.get()
