"""
Created on 2013-5-10
@author: Bobi Pu, bobi.pu@usc.edu
"""

from argparse import ArgumentParser
from DataExporter import DataExporter
from setting import *

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-t', '--type', choices=['pos', 'neg', 'article', 'cluster', 'frequent'], required=True,
                        help='export data type, one of pos, neg, article, cluster, frequent. e.g. "-t pos"')
    exportType = parser.parse_args().type
    de = DataExporter()
    print 'Start to export....'
    print 'Done!' 