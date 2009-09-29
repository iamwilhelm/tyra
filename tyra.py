#!/usr/bin/python

import redis

def _toStrings(list):
    return map(lambda x: str(x), list)

def _under(str):
    return str.replace(' ','_')
        
class Tyra:
    def __init__(self, db=0):
        '''creates the db reference'''
        self.db = redis.Redis(host='68.55.32.96', db=db)

    def _expandCategory(self, dataset, dimensions):
        """
        >>> tyra = Tyra(2)
        >>> print tyra._expandCategory('Oil', ['Category', 'State', 'Year'])
        ['State', 'Year', 'production', 'consumption|motor', 'consumption|heat', 'consumption', 'consumption|kill']
        >>> print tyra._expandCategory('Oil', ['State', 'Year'])
        ['State', 'Year']
        """
        if 'Category' in dimensions:
            dimensions.remove('Category')
            dimensions += _toStrings(self.db.smembers(dataset+'||Category'))
        return dimensions

    def lookup(self, searchStr):
        """
        >>> tyra = Tyra(2)
        >>> print tyra.lookup('Oil')
        ['Oil||production', 'Oil||consumption|motor', 'Oil||consumption|heat', 'Oil||consumption', 'Oil||consumption|kill']
        >>> print tyra.lookup('Gold')
        []
        >>> print tyra.lookup('heat')
        ['Oil||consumption|heat']
        >>> print tyra.lookup('consumption')
        ['Oil||consumption|motor', 'Oil||consumption|heat', 'Oil||consumption', 'Oil||consumption|kill']
        """
        ret = []

        # get list of all datasets
        datasets = _toStrings(self.db.smembers('datasets'))

        # check all dimensions for search string
        for dd in datasets:
            dimensions = list(self.db.smembers(dd+'||dimensions'))
            dimensions = self._expandCategory(dd, dimensions)
            for dim in dimensions:
                # add any that match, look inside category
                if searchStr in dim:
                    ret.append(dd+'||'+dim)

        # filter datasets by search string
        for dd in datasets:
            if searchStr in dd:
                # check the rest, skip state and year, look inside category
                for dim in dimensions:
                    if not dim == 'State' and not dim == 'Year':
                        ret.append(dd+'||'+dim)

        return _toStrings(ret)


    def getData(self, dimension, xAxis=None, xAxisLabels=None, zAxis=None):
        """
        >>> tyra = Tyra(2)
        >>> print tyra.getData('Oil||production')
        {'source': 'Oil Companies', 'xAxis': 'Year', 'units': 'Barrels', 'xAxisLabels': ['1995', '1996', '1997', '1998', '1999', '2000'], 'data': [2592.8677400000001, 2562.5572099999999, 2536.9843700000001, 2506.7973999999999, 2478.74694, 2450.3770800000002], 'dimension': 'Oil||production'}
        """
        # get xAxis, check default if not passed in
        dataset, dim = dimension.split('||')
        if xAxis == None:
            xAxis = str(self.db.get(dataset+'||default'))

        # get dimensions for this dataset
        dimensions = _toStrings(list(self.db.smembers(dataset+'||dimensions')))
        dimensions.sort()

        # get the keys for looking up the data
        keyList = dict()
        if dim not in dimensions and 'Category' in dimensions:
            keyList['Category'] = dim

        # get slice indices
        slice = _toStrings(self.db.smembers(dataset+'||'+xAxis))

        # summing on states, FIXME
        keyList['State'] = 'United States'

        ret = dict()
        data = []
        for ss in slice:
            keyList[xAxis] = ss
            key = dataset+'||'+'||'.join(map(lambda x: keyList[x], dimensions))
            key = key.replace(' ','_')
            data.append(float(self.db.get(key)))

        # populate the return hash
        ret['dimension'] = dimension
        ret['xAxis'] = xAxis
        ret['xAxisLabels'] = slice
        ret['data'] = data
        ret['units'] = str(self.db.get(dataset+'||units'))
        ret['source'] = str(self.db.get(dataset+'||source'))
        return ret
# run tests
if __name__ == '__main__':
    import doctest
    doctest.testmod()
