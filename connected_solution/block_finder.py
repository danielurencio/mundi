import geopandas as gpd, pandas as pd, numpy as np
from datetime import datetime
from shapely.geometry import Point


class BlockFinder:

    def __init__(self,batch,AddressFinder):

        self.cves = batch[0];
        self.cve_ent, self.cve_mun = list(map(self._parse_cves,enumerate(self.cves)))

        self.df = self._parse_df(batch[1])

        self.af = AddressFinder
        self._generate_where_condition()


    @staticmethod
    def _parse_df(df):

        geo_df = gpd.GeoDataFrame(df.copy())

        def parse_geometry(s):

            coordinates = s[s.find("(")+1:s.find(")")].split()
            coordinates = [ float(d) for d in coordinates ]

            point = Point(coordinates)
            return point;

        geo_df['geometry'] = geo_df.geometry.map(parse_geometry)
        geo_df.drop('index_right',axis=1,inplace=True)
        geo_df.crs = { 'init':'epsg:4326' }

        return geo_df 



    @staticmethod
    def _parse_cves(x):

        str_ = str(x[1])

        if x[0] ==  0:
            str_ = '0' + str_ if len(str_) < 2 else str_

        else:
            if len(str_) < 2:
                str_ = '00' + str_

            elif len(str_) < 3:
                str_ = '0' + str_


        return str_


    def _generate_where_condition(self):

        props = "JSON:features[0].properties"
        ent_cond = f"{props}.CVE_ENT='{self.cve_ent}'"
        mun_cond = f"{props}.CVE_MUN='{self.cve_mun}'"

        where_cond = f'where {ent_cond} and {mun_cond}'

        return where_cond;


    def _get_blocks(self):

        where_condition = self._generate_where_condition()
        af = self.af
        print('querying SF')
        blocks = af._get_map_from_sf('blocks',where_condition=where_condition)

        return blocks;


    def _join_blocks(self):
        start = datetime.now()
        print('getting blocks')
        self.blocks = self._get_blocks()
        print('joining blocks')
        joined = gpd.sjoin(self.df,self.blocks)
        not_joined = self.df[~self.df.merchant_id.isin(joined.merchant_id)]
        
        finish = datetime.now()

        print((finish - start).total_seconds())

        return joined,not_joined



    @staticmethod
    def extract_polygon_points(p):
        p_ = p.boundary.coords.xy
        p_ = [ Point((p_[0][i],p_[1][i])) for i in range(len(p_[0])) if i % 10 == 0 ]
        return p_;



    def find_blocks(self):
        joined, not_joined = self._join_blocks()
        #CVEGEO_right is the one we care about
        joined = joined[['merchant_id','geometry','CVEGEO_right']]\
                    .rename(columns={ 'CVEGEO_right':'CVEGEO' })
        blocks_points = ( self.blocks.CVEGEO, self.blocks['geometry'].map(self.extract_polygon_points) )
        blocks_points = [ d.to_list() for d in blocks_points ]
        
        print(len(blocks_points[0]))

        def find_missing(ppp):
            data = [ (i,min(list(map(lambda x:ppp.distance(x),b)))) for i,b in enumerate(blocks_points[1]) ]
            data = np.array(data)
            df = pd.DataFrame({ 'ix': data[:,0], 'closest_point':data[:,1]  })
            ix = df[df.closest_point == df.closest_point.min()]['ix'].values[0]
            return blocks_points[0][int(ix)]

        print('find missing')

        not_joined.loc[:,'CVEGEO'] = not_joined['geometry'].map(find_missing)
        not_joined = not_joined[['merchant_id','geometry','CVEGEO']]

        return pd.concat([joined,not_joined])


    
