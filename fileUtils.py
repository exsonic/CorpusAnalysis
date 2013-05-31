"""
Created on 2013-5-8
@author: Bobi Pu, bobi.pu@usc.edu
"""
import os, shutil, re
from setting import *
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

def loadAllXMLtoDB(inputDir):
	#have folder and p, pa info, insert after get
	db = DBController()
	for dirName, _, fileNames in os.walk(inputDir):
		print(dirName)
		for fileName in fileNames:
			try:
				fileAbsPath = getAbsPath(dirName, fileName)
				for articleDict in parseArticleFromXML(fileAbsPath):
					if db.isArticleDuplicate(articleDict['tailParagraph']):
						continue
					articleDict['companyFolder'] = dirName.split('/')[1]
					articleDict['subFolder'] = dirName.split('/')[2] if len(dirName.split('/')) > 2 else None
					db.insertArticle(articleDict)
			except Exception as e:
				print e, dirName, fileName

def parseArticleFromXML(fileDir):
	try:
		#remove the namespace first, otherwise we can't use elementTree
		xmlString = ''.join(open(fileDir).readlines())
		xmlString = re.sub(' xmlns="[^"]+"', '', xmlString, count=1)
		root = ElementTree.fromstring(xmlString)
	except Exception as e:
		print e, '.Invalidate XML file. ', fileDir
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
			articleDict['id'] = articleId
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

def convertDictTextEncoding(inputDict):
	for k, v in inputDict.iteritems():
		if isinstance(v, str) or isinstance(v, unicode):
			inputDict[k] = v.encode(ENCODE_UTF8)
	return inputDict

def getAllSubTagText(article, tagName):
	content = ''
	tag = article.find(tagName)
	if tag is not None:
		for subTag in tag:
			content += subTag.text
	return content

def getCodeList(article, tagName):
	codeList = []
	for company in article.iter(tagName):
		codeList.append(company.attrib['code'])
	return codeList

def getTagText(article, tagName):
	tag = article.find(tagName)
	return tag.text if tag is not None else ''

def loadAtrbSentenceToDB(inputDir):
	db = DBController()
	for dirName, _, fileNames in os.walk(inputDir):
		for fileName in fileNames:
			try:
				sentenceDict = parseSentenceFromAtrbFile(dirName, fileName)
				db.insertAnnotatedSentence(sentenceDict)
			except Exception as e:
				print e, dirName, fileName

def getAtrbTypeKeyFromFolderName(folderName):
	if folderName == 'External':
		return ATRB_EX
	elif folderName == 'Internal':
		return ATRB_IN
	elif folderName == 'None':
		return ATRB_NO
	else:
		raise Exception('invalid folderName')

def parseSentenceFromAtrbFile(dirName, fileName):
	articleId = fileName.split('_')[0]
	atrbType = getAtrbTypeKeyFromFolderName(dirName.split('/')[-1])
	fileAbsPath = getAbsPath(dirName, fileName)
	with open(fileAbsPath) as f:
		content = f.readline().strip().encode(ENCODE_UTF8)
	sentenceDict = {'articleId' : articleId, 'content' : content, 'atrb' : atrbType}
	return sentenceDict

def loadEngagerAndCompanyToDB(filePath):
	with open(filePath, 'rU') as f:
		reader = csv.reader(f)
		CEOList, companyList = [], []
		for line in reader:
			CEOList.append(line[-1])
			companyList.append(line[-2])
		#add analyst to engager
		CEOList = list(set(CEOList))
		companyList = list(set(companyList))
		analystList = ['analyst', 'Analyst']
		db = DBController()
		db.insertEngager(CEOList, ENGAGER_CEO)
		db.insertEngager(analystList, ENGAGER_ANALYST)
		db.insertCompany(companyList)



if __name__ == '__main__':
	pass
	# loadCEOAndCompanyFromFile('corpus/CEO.csv')