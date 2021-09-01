from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy import create_engine, bindparam
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.sql import and_, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData, Table, Column, Index
from sqlalchemy import Integer, Text, Float, DateTime, Date
from sqlalchemy.inspection import inspect

class OrmDatabase:
	metadata = None
	table_dict = None
	record = None

	def __init__(self, schema):
		self.engine = None
		self.conn = None
		self.session = None
		self.generate_table(schema)

	@classmethod
	def generate_table(cls, schema):
		Base = declarative_base()
		type_map = {
			'TEXT': Text,
			'INT': Integer,
			'INTEGER': Integer,
			'DATETIME': DateTime,
			'DATE': Date,
			'FLOAT': Float,
			'REAL': Float,
		}

		cls.metadata = MetaData()
		cls.table_dict = {}
		cls.record = type('record_dict', (), {})()
		for name, config in schema.items():
			if not any(col['attr'].get('primary_key', False) for col in config['columns']):
				config['columns'].insert(0, { 'name': 'id', 'type': 'INT', 'attr': { 'primary_key': True } })
			cls.table_dict[name] = Table(
					name,
					cls.metadata,
					*[Column(
						column_desc['name'],
						type_map[column_desc['type']],
						**column_desc['attr']
					) for column_desc in config['columns']],
					*[Index(
						index_desc['name'],
						*index_desc['columns']
					) for index_desc in config.get('index', [])],
					extend_existing=True
				)

			def init(self,*arg,**kwargs):
				for k,v in kwargs.items():
					setattr(self, k, v)

			def key_iter(self):
				for c in self.__table__.c:
					yield c.name, getattr(self, c.name)

			basic_attr = {
				'__tablename__': name,
				'__table__': cls.table_dict[name],
				'__repr__': lambda self: '%s[%s]' % (name, ','.join('%s: %s' % (c.name, getattr(self, c.name)) for c in self.__table__.columns)),
				'__iter__': key_iter,
				'__init__': init
			}
			basic_attr.update({c.name: None for c in cls.table_dict[name].columns})
			setattr(cls.record, name, type(name, (Base,DeferredReflection), basic_attr))
			mapper(getattr(cls.record, name), cls.table_dict[name])

	def connect(self, db_path, app=None):
		DB_CONNECT_STRING = 'sqlite:///' + db_path
		if app is None:
			self.engine = create_engine(DB_CONNECT_STRING, echo=False)
			self.conn = self.engine.connect()
			self.metadata.create_all(bind=self.engine)
			self.session = sessionmaker(bind=self.engine)()
		else:
			app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
			db = SQLAlchemy(app)
			self.engine = db.create_engine(DB_CONNECT_STRING, {})
			self.session = db.scoped_session(sessionmaker(bind=self.engine))()

	def close(self):
		self.session.close()
		self.session = None

	def execute(self, sql, params):
		try:
			self.session.execute(sql, params)
		except sqlalchemy.exc.OperationalError as e:
			print(e)

	def append_where(self, cols, stmt, attr, fields=None, additional_where=None):
		clause_list = []

		if additional_where is not None:
			clause_list.append(text(additional_where))

		if fields is None:
			fields = [col.name for col in cols]

		for field in [field for field in fields if field in attr]:
			val = attr[field]
			if type(val) == list:
				clause_list.append(cols[field].in_(val))
			elif type(val) == tuple and len(val) == 2:
				clause_list.append(cols[field].between(val[0], val[1]))
			else:
				clause_list.append(cols[field] == val)
		return stmt.where(and_(*clause_list))

	def get_sufficient_fields(self, table, attrs):
		fields = set(key.name for key in inspect(table).primary_key)
		if fields - attrs:
			return [col.name for col in table.columns]
		else:
			return fields

	def fetchone(self, table_name, attr):
		table = self.table_dict[table_name]
		fields = self.get_sufficient_fields(table, attr.keys())
		stmt = self.append_where(table.columns, table.select(), attr, fields=fields)
		result = self.session.execute(stmt)
		item = result.fetchone()

		if item is None:
			return self.insert(table, attr)

		return item

	def fetchall(self, table, attr, order_by=None, extra=None):
		fields = set(key.name for key in inspect(self.table_dict[table]).primary_key)
		if fields - attr.keys():
			fields = [col.name for col in self.table_dict[table].columns]

		stmt = self.append_where(self.table_dict[table].columns, self.table_dict[table].select(), attr, fields=fields, additional_where=extra)
		if order_by is not None:
			stmt = stmt.order_by(order_by)

		result = self.session.execute(stmt)
		return result.fetchall()

	def insert(self, table, attr, id_field='id'):
		if type(attr) != list:
			attr = [attr]

		fields = list(set(col.name for col in self.table_dict[table].columns).intersection(attr[0].keys()))
		id_field = [col.name for col in self.table_dict[table].primary_key]

		try:
			stmt = self.table_dict[table].insert(prefixes=['OR REPLACE']).values(**{field: bindparam(field) for field in fields})
			cursor = self.session.execute(stmt, attr)
			self.session.commit()
			
			if isinstance(id_field, list):
				fields = id_field
				inserted_primary = {k: attr[0][k] if k in attr[0] else cursor.lastrowid for k in id_field}
			elif 'id' in attr[0]:
				fields = ['id']
				inserted_primary = {'id': attr[0]['id']}
			else:
				fields = ['id']
				inserted_primary = {'id': cursor.lastrowid}

			stmt = self.append_where(self.table_dict[table].columns, self.table_dict[table].select(), inserted_primary, fields=fields)

			return self.session.execute(stmt, inserted_primary).fetchone()
		except  SQLAlchemyError as e:
			print(e)

	def delete(self, table, attr):
		try:
			stmt = self.append_where(self.table_dict[table].columns, self.table_dict[table].delete(), attr)
			self.session.execute(stmt, attr)
			self.session.commit()
		except sqlite3.Error as e:
			print(e)
