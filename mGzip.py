#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2015-08-24 17:41:23
# @Author  : Jintao Guo
# @Email   : guojt-4451@163.com


import os
import sys
import multiprocessing
import glob
import tempfile


def get_args():
    """Get arguments from commond line"""
    try:
        import argparse
    except ImportError as imerr:
        print("\033[1;31m" + str(imerr) + " \033[0m")
        sys.exit()

    parser = argparse.ArgumentParser(fromfile_prefix_chars='@',
                                     description="description")
    parser.add_argument("-p",
                        metavar="INT",
                        default=1,
                        dest="processes_number",
                        type=str,
                        help="gzip multiple samples simultaneously [1]")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-i",
                       metavar="File",
                       dest="input_file",
                       help="input file to gzip")
    group.add_argument("-I",
                       metavar="Files",
                       dest="input_list",
                       help="list of input files to gzip")
    group.add_argument("-r",
                       metavar="Regular_expression",
                       type=str,
                       dest="input_re",
                       help="input files to gzip")

    clustered_group = parser.add_argument_group("Clustered")
    clustered_group.add_argument("--qsub",
                                 action="store_true",
                                 default=False,
                                 dest="qsub",
                                 help="run crest in cluster [False]")
    clustered_group.add_argument("--nodes",
                                 metavar="STR",
                                 dest="node_name",
                                 type=str,
                                 help="name of nodes (e.g: n1,n2,...)")
    clustered_group.add_argument("-n",
                                 metavar="INT",
                                 default=1,
                                 dest="nodes_number",
                                 type=int,
                                 help="number of nodes [1]")

    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    else:
        return args


def gzip(f):
    print("gzip " + f)
    os.system("gzip " + f)


def qsub_gzip(f):
    ftmp = tempfile.NamedTemporaryFile()
    ftmp.write("#!/bin/bash\n")
    ftmp.write("#PBS -o " +
               os.path.split(os.path.realpath(__file__))[0] + "/log\n")

    if args.node_name:
        ftmp.write("#PBS -l nodes=1:" + args.node_name + ":ppn=1,walltime=100:00:00\n")
    else:
        ftmp.write("#PBS -l nodes=1:ppn=1,walltime=100:00:00\n")

    ftmp.write("#PBS -j oe\ncd $PBS_O_WORKDIR\n")
    ftmp.write("gzip " + f)
    ftmp.seek(0)
    # print ftmp.read()
    os.system("qsub " + ftmp.name)
    ftmp.close()


def makedir(new_dir, exist_dir=None):
    """Make a directory. If it doesn't exist, handling concurrent race conditions.
    """
    if exist_dir:
        new_dir = os.path.join(exist_dir, new_dir)
    if os.path.exists(new_dir):
        print("The " + new_dir + " is already exist")
    else:
        print("Make " + new_dir)
        os.makedirs(new_dir)


def main():
    global args
    args = get_args()
    pool = multiprocessing.Pool(processes=int(args.processes_number))
    makedir(os.path.split(os.path.realpath(__file__))[0] + "/log")
    if args.input_file:
        if args.qsub:
            qsub_gzip(args.input_file)
        else:
            gzip(args.input_file)
    else:
        if args.input_list:
            with open(args.input_list) as f:
                f_list = map(lambda x: x.strip(), f.readlines())
        elif args.input_re:
            f_list = glob.glob(args.input_re)

        if args.qsub:
            pool.map(qsub_gzip, f_list)
        else:
            pool.map(gzip, f_list)
        pool.close()
        pool.join()


if __name__ == '__main__':
    sys.exit(main())
