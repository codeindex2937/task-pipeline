# Introduction
This a simple implementation of handling concurency tasks without dependency on modules like rabbitmq.

You can construct multiple worker groups, each worker group contains multiple workers and each worker handles tasks in parallel way.
And the results are passed from one worker group to the down-stream worker groups until no task remains.

Besides task workers, there're units handling additional works from the fork of workflow, called service, e.g. database service.


# Examples
```python
from tasq.pipeline.system import System
from tasq.pipeline.parallel import SimpleWorker
from tasq.stock.service import DbService
from tasq.stock.worker import StockEnumerator

class ConsoleWriter(SimpleWorker):
	def process(self, data):
		print(data)

db_cnofig = {
	'db_path': 'stock.db',
	'analyze_db_path': 'analyze.db',
}
worker_config = {
	'db': 'db_service',
}

sys = System()
db_service = sys.new_service(DbService, worker_config['db'], db_cnofig)
db_service.start()

stock_enumerator = sys.new_source(1, StockEnumerator, worker_config)
stock_enumerator.send_to(sys.new_worker_group(1, ConsoleWriter))

sys.mainloop()
```