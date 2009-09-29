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

    def _expandCategory(self, dataset, dimensions):
        """
        >>> tyra = Tyra()
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
        >>> tyra = Tyra()
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


# run tests
if __name__ == '__main__':
    import doctest
    doctest.testmod()
