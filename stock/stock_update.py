from ..pipeline.service import ServiceManager
from ..pipeline.system import System
from .service import MessageService, DbService
from .worker import CounterSingleStockFetcher, \
					SingleStockFetcher, DateGenerator, ConsoleWriter
from datetime import datetime
from dateutil.relativedelta import relativedelta


db_cnofig = {
	'db_path': 'stock.db',
	'analyze_db_path': 'analyze.db'
}
fetcher_config = {
	'db': 'db',
	'fetch_interval': 5
}

sys = System()
message_service = sys.new_service(MessageService, 'message', {})
db_service = sys.new_service(DbService, fetcher_config['db'], db_cnofig)
db_port = ServiceManager.get(fetcher_config['db'])

stock_fetcher = sys.new_worker_group(1, SingleStockFetcher, fetcher_config)
counter_fetcher = sys.new_worker_group(1, CounterSingleStockFetcher, fetcher_config)
console_writer = sys.new_worker_group(1, ConsoleWriter)

stock_fetcher.send_to(console_writer)
counter_fetcher.send_to(console_writer)

today = datetime.today()
step = relativedelta(days=1)

db_service.start()

sys.new_source(1, DateGenerator, (
	datetime.strptime(db_port.get_trade_max_date({'stock_level': 1}), '%Y-%m-%d'),
	today,
	step,
)).send_to(stock_fetcher)

sys.new_source(1, DateGenerator, (
	datetime.strptime(db_port.get_trade_max_date({'stock_level': 2}), '%Y-%m-%d'),
	today,
	step,
)).send_to(counter_fetcher)

print('start')

sys.mainloop()
