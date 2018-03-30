#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Date    : 2016-03-01 18:25:36
# @Author  : Jintao Guo
# @Email   : guojt-4451@163.com
# @Version : $Id$

"""grep chr pos ref alt from tgp.

nodes=1:ppn=1,mem=10G

input:
    loci: chr pos ref alt
    tgp_pop: China.panel, HG00403 CHS EAS male
    tgp_vcf:
"""


import sys
import pandas as pd
# import glob
snp = sys.argv[1]
tgp_vcf = sys.argv[2]
out_file = sys.argv[3]

# files = glob.glob("/share/data1/TGP/ALL.chr*")

loci = pd.read_csv(snp, sep="\t", header=None)
loci.columns = ["CHROM", "POS", "REF", "ALT"]
loci["ID"] = loci.apply(lambda x: ":".join([str(x.CHROM), str(x.POS), x.REF, x.ALT]), 1)

china = pd.read_csv("~/genokon/pop/EAS.panel", sep="\t", header=None)
item = ['CHROM', 'POS', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO']
item.extend(china[0])


def read_header(tgp_vcf):
    """Read tgp vcf header."""
    reader = pd.read_csv(tgp_vcf, compression="gzip", iterator=True, header=None)
    loop = True
    while loop:
        header = reader.get_chunk(1).ix[0, 0]
        if header.startswith("#CHROM"):
            loop = False
    return(header.lstrip("#").split("\t"))


head = read_header(tgp_vcf)

reader = pd.read_csv(tgp_vcf, compression="gzip", comment="#", sep="\t", iterator=True, header=None)
loop = True
chunkSize = 100000
chunks = []
while loop:
    try:
        df = reader.get_chunk(chunkSize)
        df.columns = head
        df.CHROM = df.CHROM.astype('object')
        df1 = df.filter(items=item)
        df1["ID"] = df1.apply(lambda x: ":".join([str(x.CHROM), str(x.POS), x.REF, x.ALT]), 1)
        df2 = pd.merge(loci, df1, on='ID')
        chunks.append(df2)
    except StopIteration:
        loop = False
        print("Iteration is stopped")

out_df = pd.concat(chunks, ignore_index=True)
out_df.to_csv(out_file, sep="\t", index=None)
