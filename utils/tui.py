import time
import unicodedata

try:
	raw_input
except:
	raw_input = input

try:
	from msvcrt import kbhit, getch
except:
	import sys
	from select import select
	def kbhit():
		rlist, wlist, xlist = select([sys.stdin], [], [], 0)
		return bool(rlist)
	def getch():
		return raw_input() + '\r'

def getTerminalSize():
	import platform
	current_os = platform.system()
	tuple_xy=None
	if current_os == 'Windows':
		tuple_xy = _getTerminalSize_windows()
	if tuple_xy is None:
		tuple_xy = _getTerminalSize_tput()
	# needed for window's python in cygwin's xterm!
	if current_os == 'Linux' or current_os == 'Darwin' or  current_os.startswith('CYGWIN'):
		tuple_xy = _getTerminalSize_linux()
	if tuple_xy is None:
		tuple_xy = (80, 25)      # default value
	return tuple_xy

def _getTerminalSize_windows():
	res=None
	try:
		from ctypes import windll, create_string_buffer

		# stdin handle is -10
		# stdout handle is -11
		# stderr handle is -12

		h = windll.kernel32.GetStdHandle(-12)
		csbi = create_string_buffer(22)
		res = windll.kernel32.GetConsoleScreenBufferInfo(h, csbi)
	except:
		return None
	if res:
		import struct
		(bufx, bufy, curx, cury, wattr,
		 left, top, right, bottom, maxx, maxy) = struct.unpack("hhhhHhhhhhh", csbi.raw)
		sizex = right - left + 1
		sizey = bottom - top + 1
		return sizex, sizey
	else:
		return None

def _getTerminalSize_tput():
	# get terminal width
	# src: http://stackoverflow.com/questions/263890/how-do-i-find-the-width-height-of-a-terminal-window
	try:
		import subprocess
		proc=subprocess.Popen(["tput", "cols"],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
		output=proc.communicate(input=None)
		cols=int(output[0])
		proc=subprocess.Popen(["tput", "lines"],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
		output=proc.communicate(input=None)
		rows=int(output[0])
		return (cols,rows)
	except:
		return None


def _getTerminalSize_linux():
	def ioctl_GWINSZ(fd):
		try:
			import fcntl, termios, struct, os
			cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,'1234'))
		except:
			return None
		return cr
	cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
	if not cr:
		try:
			fd = os.open(os.ctermid(), os.O_RDONLY)
			cr = ioctl_GWINSZ(fd)
			os.close(fd)
		except:
			pass
	if not cr:
		try:
			cr = (env['LINES'], env['COLUMNS'])
		except:
			return None
	return int(cr[1]), int(cr[0])

class TextUserInterface:
	terminal_width, h = getTerminalSize()
	prompt_prefix = ''
	progress_title = ''
	progress_total = 0
	progress_current = 0
	progress_width = 5
	input = ''
	display_buffer = '>>>>'
	dislay_counter = 0

	@classmethod
	def set_prompt_text(cls, text):
		cls.prompt_prefix = text

	@classmethod
	def progress(cls, title, current=0, total=1):
		cls.progress_title = title
		cls.progress_current = current
		cls.progress_total = total
		if current == total:
			cls.msg(newline=True)
			cls.progress_title = ''
			cls.progress_total = 0
			cls.progress_current = 0
		else:
			cls.msg()

	@classmethod
	def done(cls):
		cls.progress(cls.progress_title, 1, 1)

	@classmethod
	def print(cls, msg):
		cls.msg(msg, newline=True)

	@staticmethod
	def visible_additional_length(unistr):
		return sum(1 for ch in unistr if unicodedata.east_asian_width(ch) in ['F', 'W', 'A'])

	@classmethod
	def msg(cls, msg='', newline=False, clear_input=False):
		prompter = cls.display_buffer[cls.dislay_counter//5]
		cls.dislay_counter = (cls.dislay_counter + 1) % 20
		if msg == '' and cls.progress_title:
			cls.progress_width = 5
			if cls.progress_current == 0 or cls.progress_total == 0:
				progress = 0
			else:
				progress = cls.progress_current * cls.progress_width // cls.progress_total
			msg = '%s[%s%s]' % (cls.progress_title, '=' * progress, ' ' * (cls.progress_width - progress))

		f = '\r%%%ds\r%%s%%s%%s' % (cls.terminal_width - cls.visible_additional_length(msg) - 1)

		print((f + '') % (
				msg,
				cls.prompt_prefix,
				prompter,
				cls.input
			), end='')

		if clear_input:
			cls.input = ''

		if newline:
			clean_len = 0 if clear_input else len(cls.prompt_prefix) + len(prompter) + len(cls.input)
			print('\r%s\n%s%s%s' % (
					' ' * clean_len,
					cls.prompt_prefix,
					prompter,
					cls.input
				), end='')


	@classmethod
	def prompt(cls, refresh_rate=0.1):
		while True:
			while not kbhit():
				time.sleep(refresh_rate)
			else:
				text = getch()
				try:
					if isinstance(text, bytes):
						text = text.decode()
					for ch in text:
						if ord(ch) == 0x8:
							cls.input = cls.input[:-1]
						else:
							cls.input += ch
				except:
					pass
			if not cls.input.endswith('\r') :
				cls.msg()
			else:
				ret = cls.input.strip('\r ')
				cls.msg(newline=True, clear_input=True)
				cls.input = ''
				return ret