#!/usr/bin/python3

import os
import shutil
import sys
import pexpect
from functools import lru_cache
from argparse import ArgumentParser

'''
--
Path = XXXX.7z
Type = 7z
WARNINGS:
There are data after the end of archive
Physical Size = 255461122
Tail Size = 38
Headers Size = 307
Method = LZMA:24
Solid = +
Blocks = 1

----------
Path = 001.wav
Size = 31971388
Packed Size = 255460815
Modified =
Attributes =
CRC = AB7FE423
Encrypted = -
Method = LZMA:24:lc4
Block = 0

Path = 002.wav
Size = 15985724
Packed Size =
Modified =
Attributes =
CRC = E741FBC3
Encrypted = -
Method = LZMA:24:lc4
Block = 0
'''

class DataSize(object):
  UNITS = [(1, ''), (1000, 'K'), (1000000, 'M'), (1000000000, 'G')]

  def __init__(self, val=0):
    self._val = DataSize._parse_string(val) if isinstance(val, str) else int(val)

  def __str__(self):
    val = abs(self._val)
    for (unit, suffix) in DataSize.UNITS:
      if val < unit * 500:
        return '{}{:.2f}{}'.format('-' if self._val < 0 else '', val / unit, suffix)

  def __repr__(self):
    return str(self)

  def __lt__(self, rh):
    return self._val < rh._val

  def __ge__(self, rh):
    return self._val >= rh._val

  def __truediv__(self, rh):
    return self._val / rh._val

  def __mul__(self, rh):
    return DataSize(self._val * rh)

  def __add__(self, rh):
    return DataSize(self._val + rh._val)

  def __sub__(self, rh):
    return DataSize(self._val - rh._val)

  def __bool__(self):
    return self._val != 0

  def __int__(self):
    return self._val

  @staticmethod
  def _parse_string(val):
    for (unit, suffix) in reversed(DataSize.UNITS):
      if val.endswith(suffix):
        return int(float(val[0:-len(suffix)]) * unit) if suffix else int(val)
    raise RuntimeError("Invalid data size '{}'. Should be [0-9]+[KMG]?".format(val))


class Fs(object):
  @staticmethod
  def list_files(paths):
    queue = list()
    queue.extend(paths)
    while queue:
      to_scan = queue.pop(0)
      if os.path.isfile(to_scan):
        yield (to_scan, Fs.file_size(to_scan))
      else:
        for entry in os.scandir(to_scan):
          full_path = entry.path
          if entry.is_dir():
            queue.append(full_path)
          elif entry.is_file():
            yield (full_path, DataSize(entry.stat().st_size))

  @staticmethod
  def file_size(path):
    return DataSize(os.stat(path).st_size)


class ArchivedFile(object):
  def __init__(self, path):
    self._path = path

  def _set_property(self, name, value):
    if name == 'Size':
      self._size = DataSize(value)
    elif name == 'Packed Size':
      self._packed_size = DataSize(value) if value else DataSize()
    elif name == 'Attributes':
      self._attributes = value
    elif name == 'Block':
      self._block = int(value) if value else None

  def get_path(self):
    return self._path

  def get_unpacked_size(self):
    return self._size

  def get_packed_size(self):
    return self._packed_size

  def is_unsupported(self):
    return self._attributes.startswith('D')


class Archive(object):
  def __init__(self, path):
    self._path = path
    self._files = dict()
    Archive._fill(path, self)
    self._cleanup_unsupported_files()

  def _set_property(self, name, value):
    if name == 'Solid':
      self._solid = value == '+'
    elif name == 'Method':
      self._method = value
    elif name == 'Blocks':
      self._blocks = int(value)

  def get_path(self):
    return self._path

  def is_solid(self):
    return self._solid

  def get_method(self):
    return self._method

  @lru_cache(maxsize=None)
  def get_unpacked_size(self):
    return sum(self._get_files_sizes(), DataSize())

  @lru_cache(maxsize=None)
  def get_packed_size(self):
    return sum([x.get_packed_size() for x in self._files.values()], DataSize())

  @lru_cache(maxsize=None)
  def get_blocks_sizes(self):
    blocks = [DataSize() for i in range(self._blocks)]
    for f in self._files.values():
      if f._block is not None:
        blocks[f._block] += f.get_packed_size()
    return blocks

  def get_ratio(self):
    return self.get_packed_size() / self.get_unpacked_size()

  def _add_file(self, name):
    return self._files.setdefault(name, ArchivedFile(name))

  def _cleanup_unsupported_files(self):
    self._files = dict([(name, file) for (name, file) in self._files.items() if not file.is_unsupported()])

  def get_info(self):
    if self._solid:
      solid_attrs = 'Solid ' + Archive._get_sizes_info('block', self.get_blocks_sizes())
    else:
      solid_attrs = ''
    return solid_attrs + '{} {:.2f}% ({}->{})'.format(self._method, 100 * self.get_ratio(), self.get_unpacked_size(), self.get_packed_size())

  def get_files_info(self):
    types = self.get_files_types()
    return Archive._get_sizes_info('file', self._get_files_sizes()) + 'types [{}]'.format(','.join(sorted(types)))

  @staticmethod
  def _get_sizes_info(type, sizes):
    total = sum(sizes, DataSize())
    count = len(sizes)
    return '{} {}s (min/max/avg {}/{}/{}) '.format(count, type, min(sizes), max(sizes), total * (1 / count))

  @lru_cache(maxsize=None)
  def get_files_types(self):
    return set([x._path.split('.')[-1] for x in self._files.values()])

  @lru_cache(maxsize=None)
  def _get_files_sizes(self):
    return [x.get_unpacked_size() for x in self._files.values()]

  def extract(self, out_dir):
    result_dir = os.path.join(out_dir, str(hash(self._path)))
    os.makedirs(result_dir)
    Archive._execute('7zr x "-o{}" "{}"'.format(result_dir, self._path))
    return result_dir

  @staticmethod
  def compress(input_dir, output_file, level, max_solid_block_size):
    output_path = os.path.abspath(output_file)
    solid_params = 'off' if level == 0 else 'e{}b'.format(int(max_solid_block_size))
    Archive._execute('7zr a -t7z -mx={} -myx=9 -ms={} -mmt=on "{}" .'.format(level, solid_params, output_path), cwd=input_dir)
    return Archive(output_file)

  @staticmethod
  def _fill(path, arc):
    output = Archive._execute('7zr l -slt "{}"'.format(path))
    cur_file = arc
    for line in output.split('\r\n'):
      (name, eq, value) = line.partition(' = ')
      if not eq:
        continue
      if name == 'Path' and value != path:
        cur_file = arc._add_file(value)
      cur_file._set_property(name, value)

  @staticmethod
  def _execute(cmdline, **kwargs):
    (output, status) = pexpect.run(cmdline, encoding='utf-8', timeout=600, withexitstatus=True, **kwargs)
    if status != 0:
      raise RuntimeError(cmdline + ' returned ' + str(status) + ':\n' + output)
    return output

class ArchivesCollection(object):
  @staticmethod
  def from_dir(paths):
    for path, size in Fs.list_files(paths):
      if path.endswith(('.7z', '.7Z', '.7zip')):
        try:
          arc = Archive(path)
          yield arc
        except RuntimeError as e:
          print(e)


class RecompressLogic(object):
  def __init__(self, params):
    self._params = params

  def recompress(self, archives):
    total_delta = DataSize()
    for arc in archives:
      try:
        total_delta += self._process(arc)
      except RuntimeError as e:
        print(e)
    print('Total: d={}'.format(total_delta))

  def _process(self, arc):
    files_info = arc.get_files_info()
    print(arc.get_path() + '\n ' + arc.get_info() + ' ' + files_info)
    if self._need_recompress(arc):
      after = self._recompress(arc)
      after_files = after.get_files_info()
      if files_info != after_files:
        raise RuntimeError('Files mismatch: ' + after_files)
      delta = after.get_packed_size() - arc.get_packed_size()
      print(' {}\n  d={}'.format(after.get_info(), delta))
      return delta
    else:
      print(' unchanged')
      return DataSize()

  def _need_recompress(self, arc):
    return self._is_big_solid(arc)

  def _is_big_solid(self, arc):
    return arc.is_solid() and max(arc.get_blocks_sizes()) >= self._params.max_solid_block_size

  def _recompress(self, arc):
    source_file = arc.get_path()
    output_file = os.path.join(self._params.temp_dir, os.path.basename(source_file))
    content = arc.extract(self._params.temp_dir)
    if os.path.isfile(output_file):
      os.remove(output_file)
    packed = Archive.compress(content, output_file, self._get_compression_level(arc), self._params.max_solid_block_size)
    shutil.rmtree(content)
    if self._params.replace_originals:
      os.replace(output_file, source_file)
    elif not self._params.keep_dry_run_result:
      os.remove(output_file)
    return packed

  def _get_compression_level(self, arc):
    if arc.get_ratio() >= self._params.min_unpacked_ratio and (arc.get_packed_size() + self._params.max_nonpacked_space_loss >= arc.get_unpacked_size()):
      return 0
    else:
      return self._params.compression_level


def parse_cmdline():
  parser = ArgumentParser(description='Tool to optimize collection of 7zip files')
  parser.add_argument('--max-solid-block-size', help='Limit maximum archive block size', type=DataSize, default='3M')
  parser.add_argument('--max-nonpacked-space-loss', help='Prefer unpacked archive over compressed if space loss is not over limit', type=DataSize, default='2M')
  parser.add_argument('--min-unpacked-ratio', help='Prefer unpacked archive over compressed if its ratio is more than limit', type=float, default=0.9)
  parser.add_argument('--compression-level', help='Level to use when packing', type=int, default=7)
  parser.add_argument('--temp-dir', help='Temporary directory to use', type=str, default='temp')
  parser.add_argument('--replace-originals', help='Replace originals, dry run if not specified', action='store_true')
  parser.add_argument('--keep-dry-run-result', help='Do not remove result of dry-run in temp dir', action='store_true')
  parser.add_argument('paths', metavar='path', type=str, nargs='+', help='Files and folders to process')
  return parser.parse_args()

def main():
  params = parse_cmdline()
  print(params)
  collection = ArchivesCollection.from_dir(params.paths)
  RecompressLogic(params).recompress(collection)

if __name__ == '__main__':
    main()
