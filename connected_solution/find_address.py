import snowflake.connector, geopandas as gpd, pandas as pd, json, os
import reverse_geocoder as rg
import urllib.request
from sqlalchemy import create_engine
from shapely.geometry import Point
from datetime import datetime
from block_finder import BlockFinder
from denue_handler import DenueHandler


class AddressFinder:

    def __init__(self,sf_creds,rs_conn_str):

        self.sf_creds = sf_creds
        self.rs_conn_str = rs_conn_str

        self._create_sf_connection()
        self._create_rs_connection();


    def _create_sf_connection(self):

        ctx = snowflake.connector.connect(
                user=self.sf_creds['usr'],
                password=self.sf_creds['psw'],
                account=self.sf_creds['acc']
                )

        return ctx


    def _create_rs_connection(self):

        self.rs_conn = create_engine(self.rs_conn_str);



    def _get_map_from_sf(self,map_table,where_condition=None):

        with self._create_sf_connection() as ctx:

            cs = ctx.cursor()
            cs.execute('use WAREHOUSE DEMO_WH')

            query = f'select * from DEMO_DB.PUBLIC.{map_table}'

            if where_condition:
                query += f' {where_condition}'

            cs.execute(query)
            rows = cs.fetchall()
            
            geojsons = list(map(self._parse_geojson_into_geoDF,rows))
            map_ = pd.concat(geojsons)
            map_.crs = { 'init':'epsg:4326' }

            return map_;


    def _get_transaction_geography(self):
        df = pd.read_sql('select * from clipdw_merchant.transaction_geography',self.rs_conn)

        return df;


    @staticmethod
    def _parse_geojson_into_geoDF(row):

        json_file = json.loads(row[0])
        features = json_file['features']
        geo_df = gpd.GeoDataFrame.from_features(features)

        return geo_df;


    @staticmethod
    def _turn_to_point_object(pair):

        longitude,latitude = pair
        p_ = Point([longitude,latitude])

        return p_;


    def _add_point_geometry_to_df(self,df):

        if not ('lat' in df.columns and 'long' in df.columns):
            raise ValueError('Add or rename "lat" and "long" columns!')

        df_ = df.copy()

        point_objs = df_.apply(lambda x: self._turn_to_point_object((x['long'],x['lat'])),axis=1)
        df_['geometry'] = point_objs
        df_ = gpd.GeoDataFrame(df_)
        df_.crs = { 'init':'epsg:4326' }

        return df_;


    def _find_merchants_municipalities(self):

        df = self._get_transaction_geography()
        df = self._add_point_geometry_to_df(df)
        muns = self._get_map_from_sf('municipalities')

        #muns = self._get_municipalities()

        self.merchants_with_municipality = gpd.sjoin(df,muns,op='within')

        condition = df.merchant_id.isin(self.merchants_with_municipality.merchant_id)
        self.merchants_without_municipality = df[~condition];


    def _find_location_for_missing_municipalities(self):

        pairs_of_coordinates = self.merchants_without_municipality\
                                   .apply(lambda x:(x.lat,x.long),axis=1)\
                                   .to_list()

        places = pd.DataFrame([ dict(rg.search(d)[0]) for d in pairs_of_coordinates ])

        return places;


if __name__ == '__main__':

    import Cred

    rs_conn_str = Cred.generate_redshift_conn_str('prod')
    sf_creds = Cred.obtain_snowflake_credentials('dev')

    af = AddressFinder(sf_creds, rs_conn_str)

    if 'temp_data.csv' not in os.listdir():
        af._find_merchants_municipalities()
        af.merchants_with_municipality.to_csv('temp_data.csv',index=False)

    else:
        af.merchants_with_municipality = pd.read_csv('temp_data.csv')


    # vv-- Add BLOCK to MERCHANT -- vv #

    for batch in af.merchants_with_municipality\
                    .sort_values(['CVE_ENT','CVE_MUN'],ascending=[True,True])\
                    .groupby(['CVE_ENT','CVE_MUN']):

        try:
            batch_name = f'{batch[0][0]}_{batch[0][1]}'
            print(batch_name)
            bh = BlockFinder(batch,af)
            blocks = bh.find_blocks()
            #print(blocks)
            dh = DenueHandler(blocks)
            ms_w_address = dh._add_address_attrs()
            ms_w_address.to_csv('data/' + batch_name + '.csv',index=False)
        except:
            pass

        
    # ^^-- Add BLOCK to MERCHANT -- ^^ #
