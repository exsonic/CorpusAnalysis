from argparse import ArgumentParser
from DataExporter import DataExporterMaster

if __name__ == '__main__':
	parser = ArgumentParser()
	parser.add_argument('-s', type=str, required=False, help='input the search string, get all articles that match this string.'
	                                                         'Please use double quote to warp the search String, if you want to search several keyword, plase use comma to separate.'
	                                                         'i.e. "python CmdLineTool.py -s "Professor Park"')

	args = parser.parse_args()
	searchString = args.s

	if searchString is not None:
		master = DataExporterMaster()
		master.exportKeywordSearch(searchString)


