"""
Created on 2013-5-18
@author: Bobi Pu, bobi.pu@usc.edu
"""

from DBController import DBController
from sklearn.cluster import KMeans
import numpy
from textUtils import getProcessedWordList
from setting import TOTAL_CLUSTER, BATCH_SIZE

class SentenceClusterer(object):
    def __init__(self):
        self.db = DBController()
        self.clusterer = KMeans(n_clusters=TOTAL_CLUSTER)
        
    def train(self, X):
        self.clusterer.fit(X)
        
    def predict(self, X):    
        return self.clusterer.predict(X)
        
    def updateSentenceCluster(self, clusterList, sentenceIdList):
        for sentenceId, cluster in zip(sentenceIdList, clusterList):
            self.db.updateSentenceCluster(sentenceId, int(cluster))
      
    def clusterSentenceInBatch(self, startId=0, limit=5000):
        endId, lastId = startId + BATCH_SIZE, startId + limit
        while endId < lastId:
            sentences = self.db.getSentenceInRange(startId, endId)
            self.clusterSentence(sentences)
            startId += BATCH_SIZE
            endId += BATCH_SIZE
    
    def clusterSentence(self, sentences):
        sentenceMatrix, sentenceIdList = self.getSentenceMatrixAndIdList(sentences)
        self.train(sentenceMatrix)
        clusterList = self.predict(sentenceMatrix)
        self.updateSentenceCluster(clusterList, sentenceIdList)
    
    def getSentenceMatrixAndIdList(self, sentences):
        table = self.db.getUnigramTable()
        matrix, idList = [], []
        i = 0
        for sentence in sentences:
            print i
            i += 1
            wordList = getProcessedWordList(sentence['content'])
            vector = self.getSentenceVector(table, wordList)
            matrix.append(vector)
            idList.append(sentence['_id'])
        return numpy.array(matrix), idList
    
    def getSentenceVector(self, table, wordList):
        wordIndexList = [table[word] for word in wordList]
        wordIndexList.sort()
        vector = [0] * len(table)
        for index in wordIndexList:
            vector[index] += 1
        return vector
    

# if __name__ == '__main__':
#     sc = SentenceClusterer()
#     sc.clusterSentenceInBatch(30000, 1000)