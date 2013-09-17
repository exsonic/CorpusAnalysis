"""
Created on 2013-5-8
@author: Bobi Pu, bobi.pu@usc.edu
"""
import os, shutil, re
from pyth.plugins.rtf15.reader import Rtf15Reader
from pyth.plugins.plaintext.writer import PlaintextWriter
from Settings import *
from xml.etree import ElementTree
from dateutil import parser
from DBController import DBController
import csv

def cleanFiles(inputDir):
	#remove non xml file
	for dirName, _, fileNames in os.walk(inputDir):
		for fileName in fileNames:
			if not fileName.endswith('.xml'):
				absPath = getAbsPath(dirName, fileName)
				os.remove(absPath)
	#rename every file
	for dirName, _, fileNames in os.walk(inputDir):
		if len(fileNames) > 0 and not isFileNamesIncludeLetter(fileNames):
			fileNames = sorted(fileNames, key=lambda fileName : int(fileName.split('.')[0]))
		else:
			fileNames = sorted(fileNames, key=lambda fileName : fileName.split('.')[0])
		for i, fileName in enumerate(fileNames):
			oldAbsPath = getAbsPath(dirName, fileName)
			newAbsPath = getAbsPath(dirName, str(i + 1) + '.xml')
			os.rename(oldAbsPath, newAbsPath)
	#move "pa" folder to company folder
	for dirName, _, fileNames in os.walk(inputDir):
		if dirName.endswith('pa'):
			srcAbsPath = getAbsPath(dirName)
			dstAbsPath = getAbsPath(dirName.split('/p/')[0])
			shutil.move(srcAbsPath, dstAbsPath)

def getAbsPath(relativeDirName, fileName=None):
	if fileName is None:
		return os.path.abspath(relativeDirName)
	else:
		return os.path.abspath(os.path.join(relativeDirName, fileName))

def isFileNamesIncludeLetter(fileNames):
	for fileName in fileNames:
		if fileName.find('p') != -1:
			return True
	return False

def convertDictTextEncoding(inputDict):
	for k, v in inputDict.iteritems():
		if isinstance(v, str) or isinstance(v, unicode):
			inputDict[k] = v.encode('utf-8')
	return inputDict

def getAllSubTagText(article, tagName):
	content = ''
	tag = article.find(tagName)
	if tag is not None:
		for subTag in tag:
			content += ('\n    ' + subTag.text)
	return content

def getCodeList(article, tagName):
	codeList = []
	for company in article.iter(tagName):
		codeList.append(company.attrib['code'])
	return codeList

def getTagText(article, tagName):
	tag = article.find(tagName)
	return tag.text if tag is not None else ''

def getArticleFilePath(articleDict):
	if articleDict['subFolder'] is not None:
		return articleDict['companyFolder'] + '/' + articleDict['subFolder']
	else:
		return articleDict['companyFolder']

def parseSentenceFromAtrbFile(dirName, fileName):
	articleId = fileName.split('_')[0]
	atrbType = getAtrbTypeKeyFromFolderName(dirName.split('/')[-1])
	fileAbsPath = getAbsPath(dirName, fileName)
	with open(fileAbsPath) as f:
		content = f.readline().strip().encode('utf-8')
	sentenceDict = {'articleId' : articleId, 'content' : content, 'atrb' : atrbType}
	return sentenceDict

def getAtrbTypeKeyFromFolderName(folderName):
	if folderName == 'External':
		return ATRB_EX
	elif folderName == 'Internal':
		return ATRB_IN
	elif folderName == 'None':
		return ATRB_NO
	else:
		raise Exception('invalid folderName')

def parseArticleFromXML(fileDir):
	try:
		#remove the namespace first, otherwise we can't use elementTree
		xmlString = ''.join(open(fileDir).readlines())
		xmlString = re.sub(' xmlns="[^"]+"', '', xmlString, count=1)
		root = ElementTree.fromstring(xmlString)
	except Exception as e:
		print e, '.Invalide XML file. ', fileDir
		return

	#ignore tags: section, language, reference, notes, edition, columnName, credits
	articleList = root[0][0]
	for article in articleList:
		article = article[0]
		articleDict = {}
		articleId = getTagText(article, 'accessionNo')
		if articleId == '':
			continue
		else:
			articleDict['_id'] = articleId
		articleDict['byline'] = getTagText(article, 'byline')
		articleDict['headline'] = getAllSubTagText(article, 'headline')
		articleDict['date'] = parser.parse(article.find('publicationDate')[0].text)
		articleDict['sourceCode'] = getTagText(article, 'sourceCode')
		articleDict['sourceName'] = getTagText(article, 'sourceName')
		articleDict['company'] = getCodeList(article, 'company')
		articleDict['industry'] = getCodeList(article, 'industry')
		articleDict['region'] = getCodeList(article, 'region')
		articleDict['newsSubject'] = getCodeList(article, 'newsSubject')
		articleDict['leadParagraph'] = getAllSubTagText(article, 'leadParagraph')
		articleDict['tailParagraph'] =  getAllSubTagText(article, 'tailParagraphs')
		articleDict = convertDictTextEncoding(articleDict)
		yield articleDict

def loadAllXMLtoDB(inputDir):
	#have folder and p, pa info, insert after get
	db = DBController()
	for dirName, _, fileNames in os.walk(inputDir):
		print(dirName)
		for fileName in fileNames:
			try:
				if not fileName.endswith('xml'):
					continue
				fileAbsPath = getAbsPath(dirName, fileName)
				for articleDict in parseArticleFromXML(fileAbsPath):
					#duplication check
					# if db.isArticleDuplicate(articleDict['tailParagraph']):
					# 	continue
					articleDict['filePath'] = fileAbsPath.split('Marshall_RA/')[1]
					db.saveArticle(articleDict)
			except Exception as e:
				print e, dirName, fileName


##########################################################################################################################
############ Code above is to load all article in XML format, this format is not available, right now is RTF format#######
############ Code below is to load all article in RTF format to DB                                                 #######
##########################################################################################################################

def loadAllRTFToDB(folderPath):
	db = DBController()
	for dirPath, dirNames, fileNames in os.walk(folderPath):
		for fileName in fileNames:
			if not fileName.endswith('.rtf'):
				continue
			filePath = os.path.join(dirPath, fileName)
			print(filePath)
			try:
				doc = Rtf15Reader.read(open(filePath))
				text = PlaintextWriter.write(doc).getvalue()
			except:
				continue
			lines = [line.strip() for line in text.split('\n') if line]
			articleLinesDict, articleStartIndex = {}, 0
			for i, line in enumerate(lines):
				if line.startswith('Document ') and len(line.split(' ')) == 2:
					articleId = line.split(' ')[-1]
					articleLinesDict[articleId] = lines[articleStartIndex : i]
					articleStartIndex = i + 1

			for articleId, lines in articleLinesDict.iteritems():
				bylineIndex, wordCountIndex, textStartIndex = -1, -1, -1
				for i, line in enumerate(lines):
					line = line.lower()
					if line.startswith('by '):
						bylineIndex = i
					elif line.endswith(' words'):
						wordCountIndex = i
					elif line == 'english':
						textStartIndex = i + 2

				if wordCountIndex == -1 or textStartIndex == -1 or wordCountIndex > textStartIndex:
					print(filePath + ', ' + articleId)
				else:
					articleDict = {'_id': articleId,
					               'filePath' : filePath.split('Marshall_RA/')[-1],
					               'headline': ' '.join(lines[: wordCountIndex]) if bylineIndex == -1 else ' '.join(lines[: bylineIndex]),
					               'byline' : '' if bylineIndex == -1 else lines[bylineIndex],
					               'date' : parser.parse(lines[wordCountIndex + 1]),
					               'sourceName' : lines[wordCountIndex + 2] if lines[wordCountIndex + 2].find(' AM') == -1 and lines[wordCountIndex + 2].find(' PM') == -1 else lines[wordCountIndex + 3],
					               'leadParagraph' : '',
					               'tailParagraph' : '\n'.join(lines[textStartIndex:]),
					               'sourceCode' : '', 'industry' : [], 'region' : [], 'newsSubject' : [], 'company' : []}
					db.saveArticle(articleDict)




##########################################################################################################################

def loadAtrbSentenceToDB(inputDir):
	"""
	Load all the annotated internal/external sentences to DB
	"""
	db = DBController()
	for dirName, _, fileNames in os.walk(inputDir):
		for fileName in fileNames:
			try:
				sentenceDict = parseSentenceFromAtrbFile(dirName, fileName)
				db.insertAnnotatedSentence(sentenceDict)
			except Exception as e:
				print e, dirName, fileName

def loadEngagerAndCompanyToDB(filePath):
	with open(filePath, 'rU') as f:
		db = DBController()
		reader = csv.reader(f)
		for i, line in enumerate(reader):
			line = [word.strip() for word in line]
			if i == 0:
				continue
			if db.getEngagerByName(line[5]) is None:
				engagerDict = {'name' : line[5], 'lastName' : line[6], 'type' : ENGAGER_CEO, 'gender' : line[-1]}
				db.insertEngager(engagerDict)
			if db.getCompanyByName(line[3]) is None:
				engagerDict = db.getEngagerByName(line[5])
				companyDict = {'_id' : int(line[2]), 'name' : line[3], 'shortName' : line[4], 'code' : line[0], 'CEO' : {line[1] : engagerDict['_id']}}
				db.insertCompany(companyDict)
			else:
				engagerDict = db.getEngagerByName(line[5])
				companyDict = db.getCompanyByName(line[3])
				companyDict['CEO'][line[1]] = engagerDict['_id']
				db.updateCompanyCEO(companyDict['_id'], companyDict['CEO'])


		for name in ['CEO', 'Executive']:
			engagerDict = {'name' : name, 'lastName' : name, 'type' : ENGAGER_CEO}
			db.insertEngager(engagerDict)
		for name in ['analyst']:
			engagerDict = {'name' : name, 'lastName' : name, 'type' : ENGAGER_ANALYST}
			db.insertEngager(engagerDict)

def loadBrokerageToDB(filePath):
	with open(filePath, 'rU') as f:
		db = DBController()
		reader = csv.reader(f)
		for i, line in enumerate(reader):
			if i == 0:
				continue
			line = [word.strip() for word in line]
			brokerageDict = {'_id' : line[1], 'name' : line[0], 'code' : line[2]}
			db.saveBrokerage(brokerageDict)

if __name__ == '__main__':
	# loadAllXMLtoDB('/Users/exsonic/Developer/Marshall_RA/factival_chem/')
	# loadEngagerAndCompanyToDB('corpus/CEO_company_factival_chem.csv')
	# loadAllRTFToDB('/Users/exsonic/Developer/Marshall_RA/0.chem_50')
	loadBrokerageToDB('corpus/brokerage_ids.csv')
