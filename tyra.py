#!/usr/bin/python

import redis

def _toStrings(list):
    return map(lambda x: str(x), list)

def _under(str):
    return str.replace(' ','_')
        

class Tyra:
    def __init__(self):
        '''creates the db reference'''
        self.db = redis.Redis(host='68.55.32.96')

    def getDatasetList(self):
        '''get the list of datasets available'''
        return _toStrings(list(self.db.smembers('datasets')))

    def getDimensions(self,dataset):
        '''get the dimensions for a given dataset'''
        return _toStrings(list(self.db.smembers(_under(dataset)+'||dimensions')))

    def getDimensionLabels(self, dataset, dimension):
        '''get the labels for a given dimension'''
        return _toStrings(list(self.db.smembers(_under(dataset)+'||'+_under(dimension))))

    def getProperty(self, dataset, property):
        '''get a property for a given dataset'''
        return str(self.db.get(_under(dataset)+'||'+_under(property)))

def main():
    '''prints a sort of index of what is in the db'''
    tyra = Tyra()

    datasets = tyra.getDatasetList()
    for dd in datasets:
        print ' - ' + ff
        print '   Source: ' + tyra.getProperty(ff, 'source')
        print '   URL: ' + tyra.getProperty(ff, 'url')
        print '   Units: ' + tyra.getProperty(ff, 'units')
        print '   Dimensions:'
        dims = tyra.getDimensions(ff)
        for dd in dims:
            print '   - ' + dd
            #labels = tyra.getDimensionLabels(ff, dd)
            #for ll in labels:
            #    print '     - ' + ll
        print

if __name__ == '__main__':
    main()
