#!/usr/bin/python

import redis

def _toStrings(list):
    return map(lambda x: str(x), list)

def _under(str):
    return str.replace(' ','_')
        

class Tyra:
    def __init__(self):
        '''creates the db reference'''
        self.db = redis.Redis(host='68.55.32.96', db=2)

    def lookup(self, searchStr):
        """
        >>> tyra = Tyra()
        >>> print tyra.lookup('Oil')
        ['Oil||Category||production', 'Oil||Category||consumption|motor', 'Oil||Category||consumption|heat', 'Oil||Category||consumption', 'Oil||Category||consumption|kill']
        >>> print tyra.lookup('Gold')
        []
        >>> print tyra.lookup('heat')
        ['Oil||Category||consumption|heat']
        """
        ret = []

        # get list of all datasets
        datasets = map(lambda x: str(x), self.db.smembers('datasets'))

        # check all dimensions for search string
        for dd in datasets:
            dimensions = self.db.smembers(dd+'||dimensions')
            for dim in dimensions:
                # add any that match, look inside category
                if searchStr in dim:
                    ret.append(dd+'||'+dim)
                elif 'Category' == dim:
                    for cc in self.db.smembers(dd+'||Category'):
                        if searchStr in cc:
                            ret.append(dd+'||'+dim+'||'+cc)

        # filter datasets by search string
        for dd in datasets:
            if searchStr in dd:
                # check the rest, skip state and year, look inside category
                for dim in dimensions:
                    if dim == 'State' or dim == 'Year':
                        continue
                    if dim == 'Category':
                        for cc in self.db.smembers(dd+'||Category'):
                            ret.append(dd+'||'+dim+'||'+cc)
                    else:
                        ret.append(dd+'||'+dim)

        return map(lambda x: str(x), ret)

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

    datasets = tyra.lookup('Oil')
    #for dd in datasets:
    #    print ' - ' + ff
    #    print '   Source: ' + tyra.getProperty(ff, 'source')
    #    print '   URL: ' + tyra.getProperty(ff, 'url')
    #    print '   Units: ' + tyra.getProperty(ff, 'units')
    #    print '   Dimensions:'
    #    dims = tyra.getDimensions(ff)
    #    for dd in dims:
    #        print '   - ' + dd
    #        #labels = tyra.getDimensionLabels(ff, dd)
    #        #for ll in labels:
    #        #    print '     - ' + ll
    #    print


if __name__ == '__main__':
    import doctest
    doctest.testmod()
