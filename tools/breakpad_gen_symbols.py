#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script generates symbols for breakpad from existing binaries (executable or
# shared object). This is just a wrapper from the breakpad tool, dump_syms,
# placing the resulting files.
# This should be run on binaries with symbols.

from optparse import OptionParser
import sys
import os
import shutil
import subprocess

parser = OptionParser()
parser.add_option("--dump_syms", dest="dump_syms_binary", default="dump_syms",
    help="Path to dump_syms tool")

options, args = parser.parse_args()

def run_shell_cmd(cmd):
  ret = subprocess.call(cmd, shell = True)
  if ret != 0:
    raise Exception("Failed to run cmd: %s ret=%s" % (cmd, ret))
  return 0

def mkdir_if_not_exist(p):
  if not os.path.exists(p):
    os.makedirs(p)

if __name__ == "__main__":
  if len(sys.argv) < 3:
    sys.exit("usage: [output dir] [list of binaries]");

  out_dir = sys.argv[1]
  mkdir_if_not_exist(out_dir)

  # For each binary, we need to:
  #  run the dump_syms tool, which generates a symbol file
  #  parse the top for the symbol file for a hash
  #  put the symbol file with a particular directory structure in out_dir
  #    - out_dir/<binary_name>/<HASH>/<binary_name>.sym
  tmp_sym_file = out_dir + "/sym.sym"
  for i in range(2, len(sys.argv)):
    cmd = options.dump_syms_binary + " " + sys.argv[i] + " > " + tmp_sym_file
    run_shell_cmd(cmd)
    with open(tmp_sym_file, 'r') as f:
      first_line = f.readline().strip()
      fields = first_line.split(' ')
      if len(fields) != 5:
        sys.exit("Unexpected symbol file format. First line:\n  " + first_line +
          "\nshould contain 5 fields. The binary is:\n  " + sys.argv[i])
      sym_dir = out_dir + "/" + fields[4] + "/" + fields[3]
      mkdir_if_not_exist(sym_dir)
      shutil.move(tmp_sym_file, sym_dir + "/" + fields[4] + ".sym")

