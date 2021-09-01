import sys
import os
import importlib
import fnmatch
import re
import urllib.request

error_list = []

def ensure_dir(path):
	if not os.path.exists(path):
		os.mkdir(path)

re_ext = re.compile('\.([a-z]+)$')
def fetch_img_ext(url):
	return search(re_ext, url)

def search(regex, str):
	match = regex.search(str)
	if not match:
		return ''
	return match.group(1)

def download_file(url, values = None, headers = []):
	if(url):
		headers = {
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
		}
		data = urllib.parse.urlencode(values).encode() if values else None
		req = urllib.request.Request(url, data, headers)
		resp = urllib.request.urlopen(req, timeout=5)
		return resp.read()
	else:
		raise Exception('empty url')

re_volume = re.compile('[\\/:*?"|><]')
def generate_volume_dir(output_path, title):
	return re_volume.sub('_', os.path.join(output_path, title))

def execute_python(args):
	import subprocess

	try:
		build_p = subprocess.Popen([sys.executable] + args)
		build_p.wait()
		return True
	except:
		pass

	return False

def init_module(pkg):
	for root, dirs, files in os.walk(pkg):
		for file in fnmatch.filter(files, '*.py'):
			name, ext = os.path.splitext(file)
			if name != '__init__':
				importlib.import_module('.' + name, pkg)

def install_module(module):
	try:
		import pip
	except ImportError:
		execute_python(['get-pip.py'])
		import pip

	# main before 10.0.1
	if hasattr(pip, 'main'):
		pip.main(['install', module])
		return True
	else:
		return execute_python(['-m', 'pip', 'install', module])

def prepare_module(path, cls_list=list(), target='__main__'):
	try:
		mod = importlib.import_module(path)
	except:
		install_module(path.split('.', 1)[0])
		mod = importlib.import_module(path)

	if cls_list:
		for cls in cls_list:
			setattr(sys.modules[target], cls, getattr(mod, cls))
	else:
		setattr(sys.modules[target], path.rsplit('.', 1)[-1], mod)

def import_class(path):
	for ind, name in enumerate(path.split('.')):
		if ind == 0:
			class_ = __import__(name)
		else:
			class_ = getattr(class_, name)
	return class_

def map_field(item, mapping_field):
	if not mapping_field:
		return item
	else:
		return {target_field: source_field(item) if callable(source_field) else item[source_field] for target_field, source_field in mapping_field.items()}

def raise_error(msg):
	error_list.append(msg)
	print(msg)
