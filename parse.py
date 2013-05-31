"""
Created on 2013-5-10
@author: Bobi Pu, bobi.pu@usc.edu
"""

from argparse import ArgumentParser
from SignifierParser import SignifierParser

if __name__ == '__main__':
	parser = ArgumentParser()
	parser.add_argument('-t', '--type', choices=['performance', 'negAttribution', 'engager'], required=True,
						help='parse sentence keyword type, one of performance, negAttribution, engager. e.g. "-t performance"')
	parseType = parser.parse_args().type
	parser = SignifierParser()
	print('start to parse...')
	if parseType == 'performance':
		parser.parseAllPfmSentence()
	elif parseType == 'negAttribution':
		parser.parseAllPfmNegSentenceAtrb()
	elif parseType == 'engager':
		parser.parseEngager()
	print('Done')

