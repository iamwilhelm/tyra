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

    def getFunctionList(self):
        '''get the list of functions available'''
        return _toStrings(list(self.db.smembers('_functions_')))

    def getDimensions(self,function):
        '''get the dimensions for a given function'''
        return _toStrings(list(self.db.smembers(_under(function)+'|dimensions')))

    def getDimensionLabels(self, function, dimension):
        '''get the labels for a given dimension'''
        return _toStrings(list(self.db.smembers(_under(function)+'|'+_under(dimension))))

    def getProperty(self, function, property):
        '''get a property for a given function'''
        return str(self.db.get(_under(function)+'|'+_under(property)))

def main():
    '''prints a sort of index of what is in the db'''
    smy = Tyra()

    functions = smy.getFunctionList()
    for ff in functions:
        print ' - ' + ff
        print '   Source: ' + smy.getProperty(ff, 'source')
        print '   URL: ' + smy.getProperty(ff, 'url')
        print '   Units: ' + smy.getProperty(ff, 'units')
        print '   Dimensions:'
        dims = smy.getDimensions(ff)
        for dd in dims:
            print '   - ' + dd
            #labels = smy.getDimensionLabels(ff, dd)
            #for ll in labels:
            #    print '     - ' + ll
        print

if __name__ == '__main__':
    main()
