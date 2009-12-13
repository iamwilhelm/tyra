#!/usr/bin/python

import sys, json, traceback
sys.path.append('redis')
import redis

VERSION = '0.1.3'

def _toKey(str):
    ''' lowercase and underscore a string '''
    return str.replace(' ','_').lower()
        
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
        [{'dim': 'oil|production', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`heat', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`kill', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`motor', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|total', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}]
        >>> print tyra.lookup('oil')
        [{'dim': 'oil|production', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`heat', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`kill', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`motor', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|total', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}]
        >>> print tyra.lookup('Gold')
        []
        >>> print tyra.lookup('heat')
        [{'dim': 'oil|consumption`heat', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}]
        >>> print tyra.lookup('consumption')
        [{'dim': 'oil|consumption`heat', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`kill', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'oil|consumption`motor', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}]
        >>> print tyra.lookup('Pygmy')
        [{'dim': 'whales|pygmy_whale', 'descr': 'Whale Population', 'default': 'State', 'publishDate': '1985', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_whales.csv', 'units': 'Animals'}]
        >>> print tyra.lookup('pygmy')
        [{'dim': 'whales|pygmy_whale', 'descr': 'Whale Population', 'default': 'State', 'publishDate': '1985', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_whales.csv', 'units': 'Animals'}]
        >>> print tyra.lookup('Banks')
        [{'dim': 'number_of_banks', 'descr': 'Total Number of Banks by year', 'default': 'Year', 'publishDate': '1980', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_banks.csv', 'units': 'Buildings'}]
        >>> print tyra.lookup('hump heat')
        [{'dim': 'oil|consumption`heat', 'descr': 'Info about oil', 'default': 'Year', 'publishDate': '1995', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_oil_1995.csv', 'units': 'Barrels'}, {'dim': 'whales|humpback_whale', 'descr': 'Whale Population', 'default': 'State', 'publishDate': '1985', 'sourceName': 'fake', 'url': 'http://www.graphbug.com/fakedata_whales.csv', 'units': 'Animals'}]
        """
        ret = []

        # get list of dimensions
        self.db.select(self.searchDbNum)
        dimensions = set([])
        for tt in searchStr.lower().split():
            dimensions = dimensions.union(self.db.keys("*"+tt+"*"))
        dimensions = map(str, dimensions)

        # lookup each dimension's metadata
        self.db.select(self.dataDbNum)
        for dd in dimensions:
            if '|' in dd:
                ds, dim = dd.split('|')
            else:
                ds = dd.split('|')[0]
                dim = None
            meta = json.loads(self.db.get(_toKey(ds)))
            if dim != None and dim in meta['units']:
                unitsKey = dim
            else:
                unitsKey = 'default'
            sourceVal = meta['sources'].values()[0]
            ret.append({'dim': dd,
                        'descr': str(meta['descr']),
                        'units': str(meta['units'][unitsKey]),
                        'default': str(meta['default']),
                        'url': str(sourceVal['url']),
                        'sourceName': str(sourceVal['source']),
                        'publishDate':str(sourceVal['publishDate'])
                        })

        return ret

    def getData(self, dimension, xAxis=None, xAxisLabels=None, zAxis=None):
        """
        >>> tyra = Tyra(2)
        >>> print tyra.getData('Number_of_Banks')
        {'source': ['fake'], 'xAxis': 'Year', 'units': 'Buildings', 'xAxisLabels': ['1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992'], 'data': ['5154.872312', '5149.455017', '5146.638686', '5152.872294', '5153.364456', '5153.188586', '5157.434761', '5152.977374', '5154.324641', '5149.665051', '5156.041648', '5152.083429', '5154.486292'], 'dimension': 'Number_of_Banks'}
        >>> print tyra.getData('Oil|Production')
        {'source': ['fake'], 'xAxis': 'Year', 'units': 'Barrels', 'xAxisLabels': ['1995', '1996', '1997', '1998', '1999', '2000'], 'data': ['2592.86774424', '2562.55720776', '2536.98436633', '2506.79740009', '2478.74694232', '2450.37708393'], 'dimension': 'Oil|Production'}
        >>> print tyra.getData('Oil|Production', 'State', ['Alabama', 'Arizona', 'Alaska'])
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Barrels', 'xAxisLabels': ['Alabama', 'Arizona', 'Alaska'], 'data': ['416.8514642', '120.7614268', '204.5478127'], 'dimension': 'Oil|Production'}
        >>> print tyra.getData('Number_of_Banks', 'State')
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Buildings', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': ['1312.577498', '1313.570072', '1316.950799', '1314.852173', '1315.207059', '1313.641267', '1313.48346', '1313.988841', '1315.878175', '1311.5455', '1317.073607', '1313.706553', '1314.196757', '1313.285641', '1313.228885', '1309.447477', '1312.55034', '1310.635966', '1311.729267', '1315.754047', '1317.167189', '1311.781638', '1308.10401', '1314.52436', '1310.993201', '1312.625031', '1314.583995', '1315.174767', '1312.873398', '1312.006197', '1314.726538', '1312.02684', '1312.597191', '1312.272497', '1311.519995', '1312.084874', '1316.361947', '1310.318791', '1311.883353', '1311.545868', '1313.234157', '1313.605544', '1318.224285', '1312.210721', '1315.314095', '1319.513678', '1312.946723', '1315.516379', '1313.220016', '1312.218818', '1312.925067'], 'dimension': 'Number_of_Banks'}
        >>> print tyra.getData('Whales')
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Animals', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': ['1015.390797', '1011.808456', '1016.883953', '1005.079503', '1014.404708', '1011.264968', '1010.106721', '1013.045469', '1007.345244', '1017.775308', '1014.255724', '1012.647636', '1013.669771', '1013.809064', '1012.082884', '1015.456758', '1013.186636', '1014.491271', '1012.026248', '1013.580575', '1014.14379', '1014.949526', '1014.249886', '1012.510541', '1010.635538', '1019.672097', '1007.790125', '1014.781857', '1013.23279', '1014.414678', '1017.608572', '1011.837998', '1013.041058', '1012.30774', '1013.332322', '1014.870323', '1012.899119', '1015.588605', '1014.515571', '1009.239058', '1014.816075', '1012.929284', '1012.609052', '1013.574128', '1008.954341', '1012.112172', '1012.266752', '1014.6597', '1010.328545', '1008.186508', '1013.354255'], 'dimension': 'Whales'}
        >>> print tyra.getData('Whales|Humpback_Whale')
        {'source': ['fake'], 'xAxis': 'State', 'units': 'Animals', 'xAxisLabels': ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'], 'data': ['204.494633', '201.366761', '202.077637', '200.489285', '201.796524', '202.068229', '201.657764', '204.129421', '200.693428', '204.015995', '203.907874', '200.247465', '202.57808', '203.168911', '204.624186', '201.516911', '204.377048', '201.006215', '203.174387', '203.266518', '202.9763', '201.777514', '202.695286', '204.845262', '202.082347', '204.739034', '201.525573', '203.814517', '202.260975', '203.949468', '201.424092', '200.420245', '201.234057', '202.732241', '203.527201', '204.181466', '204.075963', '203.591515', '203.948775', '200.890795', '202.147049', '201.805557', '200.541373', '204.399591', '201.101616', '203.469599', '203.412227', '201.090062', '201.231441', '200.478065', '202.939433'], 'dimension': 'Whales|Humpback_Whale'}
        >>> print tyra.getData('Number_of_Banks', 'Year', ['1980', '1981', '1982'])
        {'source': ['fake'], 'xAxis': 'Year', 'units': 'Buildings', 'xAxisLabels': ['1980', '1981', '1982'], 'data': ['5154.872312', '5149.455017', '5146.638686'], 'dimension': 'Number_of_Banks'}
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

        dataset = _toKey(dataset)
        self.db.select(self.dataDbNum)
        if not self.db.exists(dataset):
            raise Exception("dataset not found")
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

        # find sum on all dims other than xaxis
        for kk in dims:
            if kk!=xAxis and (kk!='Category' or caty==None):
                keyList[kk] = 'Total'

        # pull the actual data
        ret = dict()
        dataKeys = []
        for ss in xAxisLabels:
            keyList[xAxis] = ss
            dataKeys.append(_toKey(dataset+'|'+'|'.join( keyList[x] for x in dims )))
        data = map(str, self.db.mget(*dataKeys))

        # find units and sources
        if caty!=None and caty in meta['units']:
            units = str(meta['units'][caty])
        elif 'default' in meta['units']:
            units = str(meta['units']['default'])
        else:
            units = None

        if len(meta['otherDims'])!=0:
            source = []
            for dd,vv in otherDims.items():
                if vv=='Total':
                    source += [ str(x['source']) for x in meta['sources'].values() ]
                else:
                    source += [ str(x['source']) for x in [ x[1] for x in meta['sources'].items() if x[0] in vv ] ]
            source = list(set(source))
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

    def printMeta(self, dataset):
        dataset = _toKey(dataset)
        if not self.db.exists(dataset):
            raise Exception("dataset not found")
        meta = json.loads(self.db.get(dataset))

        print '\n'+dataset
        print str(meta['descr'])
        print ' default: ' + meta['default']
        print ' units:\n   ' + '\n   '.join( str(x[0])+' = '+x[1] for x in meta['units'].items() )
        for dd in meta['dims']:
            print ' dim: ' + dd
            print '   ' + '\n   '.join( str(x[0])+'. '+x[1] for x in enumerate(meta['dims'][dd], 1) )
        for ss in meta['sources']:
            print ' sources: ' + ss
            print'   ' + '\n   '.join( str(x[0])+' = '+x[1] for x in meta['sources'][ss].items() )

def printHelp():
    print 'Usage: tyra.py [Options]'
    print ''
    print 'Options:'
    print ' -l search       lookup given search term'
    print ' -m dataset      retrieve metadata for given dataset'
    print ' -n num          base database number'
    print ' -v              print version and exit'
    print ' -h              show this and exit'
    print ' -t              run unit tests'

if __name__ == '__main__':
    print 'tyra v' + VERSION
    
    # parse command line args
    if len(sys.argv) == 1 or sys.argv[1] == '-h':
        printHelp()
        sys.exit(0)

    lookup = False
    meta = False
    dbNum = 2

    try:
        ii = 1
        while ii<len(sys.argv):
            if sys.argv[ii] == '-t':
                import doctest
                doctest.testmod()
                sys.exit(0)
            elif sys.argv[ii] == '-v':
                sys.exit(0)
            elif sys.argv[ii] == '-l':
                if not len(sys.argv) > ii+1:
                    raise Exception('-l option requires search term')
                ii+=1
                search = sys.argv[ii]
                lookup = True
            elif sys.argv[ii] == '-m':
                if not len(sys.argv) > ii+1:
                    raise Exception('-m option requires dataset name')
                ii+=1
                dataset = sys.argv[ii].split('|')[0]
                meta = True
            elif sys.argv[ii] == '-n':
                if not len(sys.argv) > ii+1:
                    raise Exception('-n option requires number argument')
                ii+=1
                dbNum = int(sys.argv[ii])
            ii+=1

        if lookup:
            dw = Tyra(dbNum)
            print '\n'.join( str(x[0])+'. '+x[1] for x in enumerate([ x['dim'] for x in dw.lookup(search) ], 1) )
        elif meta:
            dw = Tyra(dbNum+1)
            dw.printMeta(dataset)
        else:
            printHelp()

    except Exception,ex:
        print 'FAIL: ' + str(ex)
        #print traceback.print_exc()
