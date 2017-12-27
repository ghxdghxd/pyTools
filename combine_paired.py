#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Date    : 2015-07-23 11:35:01
# @Author  : Jintao Guo
# @Email   : guojt-4451@163.com

r"""Calls files pairs.

if they are completely the same except for one has _1 and the other has _2 \
returns a list of tuples of pairs or singles.
"""

import sys
import glob

version = "3.0"


def get_args3():
    """Get arguments from commond line."""
    try:
        import argparse
    except ImportError as imerr:
        print("\033[1;31m" + str(imerr) + " \033[0m")
        sys.exit()

    parser = argparse.ArgumentParser(usage="%(prog)s",
                                     fromfile_prefix_chars='@',
                                     description=__doc__)

    parser.add_argument("-a",
                        metavar="Regular_expression",
                        type=str,
                        dest="paired_1_re",
                        help="paired_1 files Regular_expression")

    parser.add_argument("-b",
                        metavar="Regular_expression",
                        type=str,
                        dest="paired_2_re",
                        help="paired_2 files Regular_expression")

    parser.add_argument("-o",
                        metavar="FILE",
                        dest="output_file",
                        help="output file ")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    else:
        return parser.parse_args()


def main():
    """Main."""
    global args
    args = get_args3()
    paired_1 = glob.glob(args.paired_1_re)
    paired_2 = glob.glob(args.paired_2_re)

    name_1 = args.paired_1_re.split("/")[-1][2:]
    name_2 = args.paired_2_re.split("/")[-1][2:]

    name_1_list = list(map(lambda x: x.strip(name_1), paired_1))
    name_2_list = list(map(lambda x: x.strip(name_2), paired_2))

    name_int = set(name_1_list).intersection(name_2_list)

    merge = map(lambda x: x + name_1 + "\t" + x + name_2, name_int)

    with open(args.output_file, "w") as f:
        f.write("\n".join(merge))

if __name__ == '__main__':
    sys.exit(main())
