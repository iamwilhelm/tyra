#!/usr/bin/python

import sys
sys.path.append('redis')
import redis

def _toStrings(list):
    return [str(x) for x in list]

def _under(str):
    return str.replace(' ','_')
        
class Tyra:
    def __init__(self, db=0, host='68.55.32.96'):
        '''creates the db reference'''
        self.db = redis.Redis(host=host, db=db)

    def _expandCategory(self, dataset, dimensions):
        """
        >>> tyra = Tyra(2)
        >>> print tyra._expandCategory('Oil', ['Category', 'State', 'Year'])
        ['State', 'Year', 'Consumption|Motor', 'Production', 'Consumption|Heat', 'Consumption', 'Consumption|Kill']
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
        ['Oil||Consumption|Motor', 'Oil||Production', 'Oil||Consumption|Heat', 'Oil||Consumption', 'Oil||Consumption|Kill']
        >>> print tyra.lookup('oil')
        ['Oil||Consumption|Motor', 'Oil||Production', 'Oil||Consumption|Heat', 'Oil||Consumption', 'Oil||Consumption|Kill']
        >>> print tyra.lookup('Gold')
        []
        >>> print tyra.lookup('heat')
        ['Oil||Consumption|Heat']
        >>> print tyra.lookup('consumption')
        ['Oil||Consumption|Motor', 'Oil||Consumption|Heat', 'Oil||Consumption', 'Oil||Consumption|Kill']
        >>> print tyra.lookup('Pygmy')
        ['Whales||Pygmy Whale']
        >>> print tyra.lookup('pygmy')
        ['Whales||Pygmy Whale']
        >>> print tyra.lookup('Banks')
        []
        """
        ret = []

        searchStr = searchStr.lower()

        # get list of all datasets
        datasets = _toStrings(self.db.smembers('datasets'))
        datasets = [ _under(x) for x in datasets ]

        # check all dimensions for search string
        for dd in datasets:
            dimensions = list(self.db.smembers(dd+'||dimensions'))
            dimensions = self._expandCategory(dd, dimensions)
            for dim in filter(lambda x: searchStr in x.lower(), dimensions):
                ret.append(dd+'||'+dim)

        # filter datasets by search string. if a dataset matched, include all its data dimensions
        for dd in filter(lambda x: searchStr in x.lower(), datasets):
            dimensions = list(self.db.smembers(dd+'||dimensions'))
            dimensions = self._expandCategory(dd, dimensions)
            dataDims = filter(lambda x: not x == 'State' and not x == 'Year', dimensions)
            for dim in dataDims:
                ret.append(dd+'||'+dim)

        return _toStrings(ret)

    def _getMeta(self, metaSet, fieldName):
        """
        >>> tyra = Tyra(2)
        >>> meta = tyra.db.smembers('Whales||meta')
        >>> print tyra._getMeta(meta, 'source')
        Greenpeace
        >>> print tyra._getMeta(meta, 'url')
        http://www.gp.org/whales.csv
        >>> print tyra._getMeta(meta, 'default')
        State
        >>> print tyra._getMeta(None, 'source')
        None
        >>> print tyra._getMeta(meta, 'cow')
        None
        """
        if metaSet==None:
            return None
        fields = filter(lambda x: x.startswith(fieldName + '||'), metaSet)
        if (len(fields)==0):
            return None
        val = str(fields[0]).partition('||')[2]
        if (len(val)==0):
            return None;
        return val


    def getData(self, dimension, xAxis=None, xAxisLabels=None, zAxis=None):
        """
        >>> tyra = Tyra(2)
        >>> print tyra.getData('Oil||Production')
        {'source': 'Oil Companies', 'xAxis': 'Year', 'units': 'Barrels', 'xAxisLabels': ['1995', '1996', '1997', '1998', '1999', '2000'], 'data': [2592.8677400000001, 2562.5572099999999, 2536.9843700000001, 2506.7973999999999, 2478.74694, 2450.3770800000002], 'dimension': 'Oil||Production'}
        >>> print tyra.getData('Banks')
        {'source': 'The Fed', 'xAxis': 'Year', 'units': 'Buildings', 'xAxisLabels': ['1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992'], 'data': [5154.8723099999997, 5149.4550200000003, 5146.63868, 5152.8723, 5153.3644599999998, 5153.18858, 5157.4347600000001, 5152.9773699999996, 5154.3246399999998, 5149.6650499999996, 5156.0416500000001, 5152.0834299999997, 5154.4862899999998], 'dimension': 'Banks'}
        >>> print tyra.getData('Banks', 'State')
        {'source': 'The Fed', 'xAxis': 'State', 'units': 'Buildings', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': [1312.5775000000001, 1313.57007, 1316.9508000000001, 1314.8521699999999, 1315.20706, 1313.6412700000001, 1313.4834599999999, 1313.98884, 1315.8781799999999, 1311.5454999999999, 1317.0736099999999, 1313.7065500000001, 1314.19676, 1313.2856400000001, 1313.2288799999999, 1309.44748, 1312.55034, 1310.63597, 1311.72927, 1315.75405, 1317.1671899999999, 1311.7816399999999, 1308.10401, 1314.5243599999999, 1310.9931999999999, 1312.6250299999999, 1314.5839900000001, 1315.1747700000001, 1312.8733999999999, 1312.0062, 1314.7265400000001, 1312.02684, 1312.59719, 1312.2725, 1311.51999, 1312.0848699999999, 1316.36195, 1310.31879, 1311.8833500000001, 1311.5458699999999, 1313.23416, 1313.60554, 1318.2242900000001, 1312.21072, 1315.3140900000001, 1319.51368, 1312.9467199999999, 1315.51638, 1313.22002, 1312.2188200000001, 1312.92507], 'dimension': 'Banks'}
        >>> print tyra.getData('Whales')
        {'source': 'Greenpeace', 'xAxis': 'State', 'units': 'Animals', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'United States', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': [1015.3908, 1011.80846, 1016.88395, 1005.0795000000001, 1014.40471, 1011.2649699999999, 1010.10672, 1013.04547, 1007.34524, 1017.77531, 1014.25572, 1012.64764, 1013.66977, 1013.80906, 1012.08288, 1015.45676, 1013.18664, 1014.49127, 1012.02625, 1013.58057, 1014.14379, 1014.94953, 1014.2498900000001, 1012.51054, 1010.63554, 1019.6721, 1007.79012, 1014.7818600000001, 1013.23279, 1014.41468, 1017.60857, 1011.838, 1013.04106, 1012.30774, 1013.33232, 1014.87032, 1012.89912, 1015.5886, 1014.51557, 1009.23906, 1014.81607, 1012.9292799999999, 1012.60905, 1013.57413, 51663.723700000002, 1008.95434, 1012.11217, 1012.26675, 1014.6597, 1010.32854, 1008.18651, 1013.35426], 'dimension': 'Whales'}
        >>> print tyra.getData('Whales||Humpback_Whale')
        {'source': 'Greenpeace', 'xAxis': 'State', 'units': 'Animals', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'United States', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': [204.49463299999999, 201.366761, 202.07763700000001, 200.489285, 201.79652400000001, 202.068229, 201.65776399999999, 204.12942100000001, 200.69342800000001, 204.015995, 203.90787399999999, 200.24746500000001, 202.57808, 203.16891100000001, 204.62418600000001, 201.51691099999999, 204.377048, 201.006215, 203.174387, 203.26651799999999, 202.97630000000001, 201.777514, 202.69528600000001, 204.84526199999999, 202.082347, 204.739034, 201.52557300000001, 203.814517, 202.260975, 203.949468, 201.424092, 200.42024499999999, 201.23405700000001, 202.73224099999999, 203.52720099999999, 204.181466, 204.075963, 203.59151499999999, 203.94877500000001, 200.890795, 202.14704900000001, 201.80555699999999, 200.54137299999999, 204.39959099999999, 10329.965899999999, 201.10161600000001, 203.46959899999999, 203.412227, 201.09006199999999, 201.23144099999999, 200.47806499999999, 202.93943300000001], 'dimension': 'Whales||Humpback_Whale'}
        >>> print tyra.getData('Banks', 'Year', ['1980', '1981', '1982'])
        {'source': 'The Fed', 'xAxis': 'Year', 'units': 'Buildings', 'xAxisLabels': ['1980', '1981', '1982'], 'data': [5154.8723099999997, 5149.4550200000003, 5146.63868], 'dimension': 'Banks'}
        """
        # set category if one is specified
        keyList = dict()
        caty = None
        if '||' in dimension:
            dataset, caty = dimension.split('||')
            keyList['Category'] = caty
        else:
            dataset = dimension
        meta = self.db.smembers(dataset+'||meta')

        # get xAxis, check default if not passed in
        if xAxis == None:
            xAxis = self._getMeta(meta, 'default')

        # get dimensions for this dataset
        dimensions = _toStrings(list(self.db.smembers(dataset+'||dimensions')))
        dimensions.sort()

        # get slice indices
        if xAxisLabels == None:
            xAxisLabels = _toStrings(list(self.db.smembers(dataset+'||'+xAxis)))
            xAxisLabels = filter(lambda x: not x=='Total', xAxisLabels)
            xAxisLabels.sort()

        # find sum on other dims
        for kk in dimensions:
            if kk==xAxis:
                continue
            if kk=='Category' and not caty == None:
                continue
            keyList[kk] = 'Total'

        ret = dict()
        data = []
        for ss in xAxisLabels:
            keyList[xAxis] = ss
            key = _under(dataset+'||'+'||'.join([ keyList[x] for x in dimensions ]))
            val = self.db.get(key)
            data.append(float(self.db.get(key)))

        # populate the return hash
        ret['dimension'] = dimension
        ret['xAxis'] = xAxis
        ret['xAxisLabels'] = xAxisLabels
        ret['data'] = data
        ret['units'] = self._getMeta(meta, 'units')
        ret['source'] = self._getMeta(meta, 'source')
        return ret

# run tests
if __name__ == '__main__':
    import doctest
    doctest.testmod()
    #tyra = Tyra(2)
    #print tyra.getData('Whales||Humpback_Whale')
