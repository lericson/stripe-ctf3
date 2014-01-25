"App"

from __future__ import print_function, unicode_literals

import os
import gc
import sys
import time
import json
import array
import codecs
import string
import threading
from optparse import OptionParser

root_path = lambda *a: os.path.join(os.path.dirname(__file__), *a)
sys.path.insert(0, root_path('purelib.zip'))

import datrie
from werkzeug import Request

import marican

gc.disable()

option_parser = OptionParser()
option_parser.add_option('--master', help='Disables debug', action='store_true')
option_parser.add_option('--id', help='Enables stillborn mode', type=int, default=0)

(opts, args) = option_parser.parse_args(sys.argv[1:])
if opts.id:
    sys.exit(0)

t_marikan_start = time.time()

def info(*a, **k):
    k['file'] = sys.stderr
    t = time.time() - t_marikan_start
    return print('[%10.2f]' % (t,), *a, **k)

min_word_len = 3

if not os.path.isfile('words.datrie'):
    info('Generating words trie')
    with open(root_path('words')) as f:
        words = [unicode(line.strip()) for line in f
                 if len(line) > min_word_len]
        words_trie = datrie.BaseTrie(string.letters)
        for i, word in enumerate(frozenset(words)):
            words_trie[word] = i
        del words
        gc.collect()
    words_trie.save('words.datrie')
else:
    info('Loading words trie')
    words_trie = datrie.BaseTrie.load('words.datrie')

info('Building match index')
words_map = dict(words_trie)
n_words = len(words_map)
match_index = [array.array(b'I') for i in xrange(n_words)]

files_index = []
indexing_paths = []

info('Ready')

class Marikan(marican.Marican):
    def _query_purelib(self, q):
        return ('{"success": true, "results": ['
              + ', '.join('"%s:%d"' % (files_index[m >> 16], m & 0xffff)
                          for m in match_index[words_map[q]])
              + ']}\n')

    def index_file(self, fp, file_index):
        items = words_trie.substring_items
        for line_no, line in enumerate(fp, 1):
            for word_index in frozenset([wi for (w, wi) in items(line)]):
                match_index[word_index].append((file_index << 16) | line_no)
        return fp.tell()

    def index_path(self, path):
        indexing_paths.append(path)
        info('Indexing', path)
        t0 = time.time()
        sizes = []
        for dirn, subdirs, fns in os.walk(path):
            subdirs[:] = filter(lambda sn: not sn.startswith('.'), subdirs)
            if not fns:
                continue
            for fn in fns:
                if fn.startswith('.'):
                    continue
                fpath = os.path.join(dirn, fn)
                file_index = len(files_index)
                files_index.append(fpath[len(path)+1:])
                with codecs.open(fpath, encoding='utf-8') as fp:
                    sizes.append(self.index_file(fp, file_index))
        gc.collect()
        info('Indexing done', path)
        info('Average speed:', (sum(sizes) / 1024.0 / 1024.0) / (time.time() - t0), 'MB/s')
        info('Matches in index:', sum(map(len, match_index)))
        info('Process info:\n' + __import__('subprocess').check_output(['ps', 'u', '-p', str(os.getpid())]))
        indexing_paths.remove(path)

    def index(self, path):
        t = threading.Thread(target=self.index_path, args=(path,))
        t.start()
        return json.dumps(dict(success=True))

    def is_indexed(self):
        indexed = bool(files_index and not indexing_paths)
        return json.dumps(dict(success=indexed))

    def healthcheck(self):
        return json.dumps(dict(success=True))

if __name__ == '__main__':
    #t = threading.Thread(target=index_path, args=('test/data/input',))
    #index_threads.append(t)
    #t.start()
    #t.join()
    app = Marikan(match_index=match_index,
                  words_map=words_map,
                  files_index=files_index)
    marican.HTTPServer(app, ('127.0.0.1', 9090)).run()