#!/usr/bin/python

import sys, json
sys.path.append('redis')
import redis

def _toStrings(list):
    return [ str(x) for x in list ]

def _under(str):
    return str.replace(' ','_')
        
class Tyra:
    def __init__(self, dbNum=0, host='68.55.32.96'):
        '''creates the db reference'''
        self.db = redis.Redis(host=host, db=dbNum)
        self.searchDbNum = dbNum
        self.dataDbNum = dbNum+1

    def lookup(self, searchStr):
        """
        >>> tyra = Tyra(2)
        >>> print tyra.lookup('Oil')
        ['oil', 'oil|consumption`heat', 'oil|consumption`kill', 'oil|consumption', 'oil|consumption`motor', 'oil|production']
        >>> print tyra.lookup('oil')
        ['oil', 'oil|consumption`heat', 'oil|consumption`kill', 'oil|consumption', 'oil|consumption`motor', 'oil|production']
        >>> print tyra.lookup('Gold')
        []
        >>> print tyra.lookup('heat')
        ['oil|consumption`heat']
        >>> print tyra.lookup('consumption')
        ['oil|consumption`heat', 'oil|consumption`kill', 'oil|consumption', 'oil|consumption`motor']
        >>> print tyra.lookup('Pygmy')
        ['whales|pygmy_whale']
        >>> print tyra.lookup('pygmy')
        ['whales|pygmy_whale']
        >>> print tyra.lookup('Banks')
        ['number_of_banks']
        >>> print tyra.lookup('hump heat')
        ['oil|consumption`heat', 'whales|humpback_whale']
        """
        ret = []

        self.db.select(self.searchDbNum)
        keys = set([])
        for tt in searchStr.lower().split():
            keys = keys.union(self.db.keys("*"+tt+"*"))

        return _toStrings(keys)

    def getData(self, dimension, xAxis=None, xAxisLabels=None, zAxis=None):
        """
        >>> tyra = Tyra(2)
        >>> print tyra.getData('Number_of_Banks')
        {'source': ['fake'], 'xAxis': 'Year', 'units': 'Buildings', 'xAxisLabels': ['1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992'], 'data': ['5154.87231', '5149.45502', '5146.63868', '5152.8723', '5153.36446', '5153.18858', '5157.43476', '5152.97737', '5154.32464', '5149.66505', '5156.04165', '5152.08343', '5154.48629'], 'dimension': 'Banks'}
        >>> print tyra.getData('Oil|Production')
        {'source': ['fake'], 'xAxis': 'Year', 'units': 'Barrels', 'xAxisLabels': ['1995', '1996', '1997', '1998', '1999', '2000'], 'data': ['2592.86774', '2562.55721', '2536.98437', '2506.7974', '2478.74694', '2450.37708'], 'dimension': 'Oil|Production'}
        >>> print tyra.getData('Oil|Production', 'State', ['Alabama', 'Arizona', 'Alaska'])
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Barrels', 'xAxisLabels': ['Alabama', 'Arizona', 'Alaska'], 'data': ['None', 'None', 'None'], 'dimension': 'Oil|Production'}
        >>> print tyra.getData('Number_of_Banks', 'State')
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Buildings', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': ['1312.5775', '1313.57007', '1316.9508', '1314.85217', '1315.20706', '1313.64127', '1313.48346', '1313.98884', '1315.87818', '1311.5455', '1317.07361', '1313.70655', '1314.19676', '1313.28564', '1313.22888', '1309.44748', '1312.55034', '1310.63597', '1311.72927', '1315.75405', '1317.16719', '1311.78164', '1308.10401', '1314.52436', '1310.9932', '1312.62503', '1314.58399', '1315.17477', '1312.8734', '1312.0062', '1314.72654', '1312.02684', '1312.59719', '1312.2725', '1311.51999', '1312.08487', '1316.36195', '1310.31879', '1311.88335', '1311.54587', '1313.23416', '1313.60554', '1318.22429', '1312.21072', '1315.31409', '1319.51368', '1312.94672', '1315.51638', '1313.22002', '1312.21882', '1312.92507'], 'dimension': 'Banks'}
        >>> print tyra.getData('Whales')
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Animals', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'United States', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': ['1015.3908', '1011.80846', '1016.88395', '1005.0795', '1014.40471', '1011.26497', '1010.10672', '1013.04547', '1007.34524', '1017.77531', '1014.25572', '1012.64764', '1013.66977', '1013.80906', '1012.08288', '1015.45676', '1013.18664', '1014.49127', '1012.02625', '1013.58057', '1014.14379', '1014.94953', '1014.24989', '1012.51054', '1010.63554', '1019.6721', '1007.79012', '1014.78186', '1013.23279', '1014.41468', '1017.60857', '1011.838', '1013.04106', '1012.30774', '1013.33232', '1014.87032', '1012.89912', '1015.5886', '1014.51557', '1009.23906', '1014.81607', '1012.92928', '1012.60905', '1013.57413', '51663.7237', '1008.95434', '1012.11217', '1012.26675', '1014.6597', '1010.32854', '1008.18651', '1013.35426'], 'dimension': 'Whales'}
        >>> print tyra.getData('Whales|Humpback_Whale')
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Animals', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'United States', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': ['204.494633', '201.366761', '202.077637', '200.489285', '201.796524', '202.068229', '201.657764', '204.129421', '200.693428', '204.015995', '203.907874', '200.247465', '202.57808', '203.168911', '204.624186', '201.516911', '204.377048', '201.006215', '203.174387', '203.266518', '202.9763', '201.777514', '202.695286', '204.845262', '202.082347', '204.739034', '201.525573', '203.814517', '202.260975', '203.949468', '201.424092', '200.420245', '201.234057', '202.732241', '203.527201', '204.181466', '204.075963', '203.591515', '203.948775', '200.890795', '202.147049', '201.805557', '200.541373', '204.399591', '10329.9659', '201.101616', '203.469599', '203.412227', '201.090062', '201.231441', '200.478065', '202.939433'], 'dimension': 'Whales|Humpback_Whale'}
        >>> print tyra.getData('Number_of_Banks', 'Year', ['1980', '1981', '1982'])
        {'source': ['fake'], 'xAxis': 'Year', 'units': 'Buildings', 'xAxisLabels': ['1980', '1981', '1982'], 'data': ['5154.87231', '5149.45502', '5146.63868'], 'dimension': 'Banks'}
        >>> print tyra.getData('Peanut_Butter|PBJ', None, ['Alabama', 'Arizona', 'Alaska'])
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Tons', 'xAxisLabels': ['Alabama', 'Arizona', 'Alaska'], 'data': ['1015.3908', '1016.88395', '1011.80846'], 'dimension': 'Peanut_Butter|PBJ'}
        """
        # set category if one is specified
        keyList = dict()
        caty = None
        if '|' in dimension:
            dataset, caty = dimension.split('|')
            keyList['Category'] = caty
        else:
            dataset = dimension

        self.db.select(self.dataDbNum)
        meta = json.loads(self.db.get(dataset))

        # get xAxis, check default if not passed in
        if xAxis == None:
            xAxis = str(meta['default'])

        # get slice indices
        if xAxisLabels == None:
            xAxisLabels = map(str, meta['dims'][xAxis])
            xAxisLabels = filter(lambda x: x!='Total', xAxisLabels)
            xAxisLabels.sort()

        # get list of sorted dimensions for this dataset
        dims = sorted(map(str, meta['dims'].keys()))

        # build otherdim key
        otherDims = {}
        for kk in dims:
            if kk in meta['otherDims']:
                if kk in xAxis:
                    otherDims[kk] = xAxisLabels
                else:
                    otherDims[kk] = 'Total'

        # TODO find sum on all dims other than xaxis
        # for now, just use the first col
        for kk in dims:
            if kk!=xAxis and (kk!='Category' or caty==None):
                keyList[kk] = str(sorted(meta['dims'][kk])[0]) #'Total'

        # pull the actual data
        ret = dict()
        data = []
        for ss in xAxisLabels:
            keyList[xAxis] = ss
            key = _under(dataset+'|'+'|'.join([ keyList[x] for x in dims ]))
            data.append(str(self.db.get(key)))

        # find units and sources
        if caty!=None and caty in meta['units']:
            units = str(meta['units'][caty])
        elif 'default' in meta['units']:
            units = str(meta['units']['default'])
        else:
            units = None

        if len(meta['otherDims'])!=0:
            source = ['dunno']
        elif 'default' in meta['sources']:
            source = [str(meta['sources']['default']['source'])]
        else:
            source = None

        # populate the return hash
        ret['dimension'] = dimension
        ret['xAxis'] = xAxis
        ret['xAxisLabels'] = xAxisLabels
        ret['data'] = data
        ret['units'] = units
        ret['source'] = source
        return ret

# run tests
if __name__ == '__main__':
    import doctest
    doctest.testmod()
    #tyra = Tyra(2)
    #print tyra.getData('Whales||Humpback_Whale')
