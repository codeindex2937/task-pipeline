from .service import ServiceManager
from .parallel import SIGNAL_STOP
import time
import signal
import threading
from collections import deque


class WorkerGroup:
	def __init__(self, worker_count, worker_type, config=None):
		self.pipe = deque()
		self.worker_list = [worker_type(self.pipe, config) for i in range(worker_count)]
		self.consumers = []
		self.started = threading.Event()
		self.source_count = 0

	def set_source_empty(self):
		self.source_count -= 1
		if self.source_count <= 0:
			for _ in self.worker_list:
				self.pipe.append(SIGNAL_STOP)
			threading.Thread(target=self.wait_and_notify_done).start()

	def wait_and_notify_done(self):
		self.started.wait()
		self.join()

		for consumer in self.consumers:
			consumer.set_source_empty()

	def is_alive(self):
		return any(worker.is_alive() for worker in self.worker_list)

	def send_to(self, worker_group):
		self.consumers.append(worker_group)
		worker_group.source_count += 1
		consumer_pipe = worker_group.pipe
		for worker in self.worker_list:
			worker.send_to(consumer_pipe)
		return worker_group

	def start(self):
		if self.started.is_set():
			return
		for worker in self.worker_list:
			worker.start()
		self.started.set()

	def abort(self):
		if not self.started.is_set():
			return
		for worker in self.worker_list:
			if worker.is_alive():
				self.pipe.appendleft(SIGNAL_STOP)
				worker.abort()

	def join(self):
		for worker in self.worker_list:
			worker.join()

class System:
	systems = []
	@classmethod
	def signal_handler(cls, signal, frame):
		for system in cls.systems:
			system.abort()

	def __init__(self):
		self.running = True
		self.services = []
		self.worker_groups = []
		self.__class__.systems.append(self)

	def abort(self):
		self.running = False

	def new_service(self, service_type, name, config):
		pipe_out, pipe_in = ServiceManager.register(service_type.Port, name)
		service = service_type(config, pipe_out, pipe_in)
		self.services.append(service)
		return service

	def new_worker_group(self, count, worker_type, config=None):
		worker_group = WorkerGroup(count, worker_type, config)
		self.worker_groups.append(worker_group)
		return worker_group

	def new_source(self, count, worker_type, config=None):
		source_group = WorkerGroup(count, worker_type, config)
		source_group.pipe.append(None)
		source_group.set_source_empty()
		self.worker_groups.append(source_group)
		return source_group

	def mainloop(self):
		for service in self.services:
			if not service.running:
				service.start()
		for worker_group in self.worker_groups:
			worker_group.start()
		while self.running:
			if all(not group.is_alive() for group in self.worker_groups):
				break
			time.sleep(1)
		for worker_group in self.worker_groups:
			worker_group.abort()
		for worker_group in self.worker_groups:
			worker_group.join()
		for service in self.services:
			service.abort()
		for service in self.services:
			service.join()

signal.signal(signal.SIGINT, System.signal_handler)
