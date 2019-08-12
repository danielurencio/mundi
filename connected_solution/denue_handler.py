import geopandas as gpd, pandas as pd, json
import urllib.request
from shapely.geometry import Point

class DenueHandler:

    def __init__(self,blocks,level='ageb'):
        self.blocks = blocks.copy()

        level_cut = {
            'loc':-7,
            'ageb':-3
        }

        self.blocks.loc[:,'CVEGEO_'] = self.blocks['CVEGEO'].map(lambda x:x[:level_cut[level]])

        self.cve_decoder = {
            'cve_ent':(0,2),
            'cve_mun':(2,5),
            'cve_loc':(5,9),
            'cve_ageb':(9,13),
            'cve_block':(13,16)
        }

    @staticmethod
    def _add_point_object(x):
        lon = float(x['Longitud'])
        lat = float(x['Latitud'])
        point = Point((lon,lat))
        x['point'] = point
        return x


    def _hit_api(self,init,end,**cves):

        all_cves = ('cve_ent','cve_mun','cve_loc','cve_ageb','cve_block')
        c =  { d:cves[d] if d in cves.keys() else '0' for d in all_cves }

        prefix = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/BuscarAreaAct'

        geoloc = f'{c["cve_ent"]}/{c["cve_mun"]}/{c["cve_loc"]}/{c["cve_ageb"]}/{c["cve_block"]}'

        sector = '0'
        subsector = '0'
        rama = '0'
        clase = '0'

        industrial_class = f'{sector}/{subsector}/{rama}/{clase}'

        unit_name = '0'
        init_ = init
        end_ = end
        unit_id = '0'

        other = f'{unit_name}/{init_}/{end_}/{unit_id}'
        token = '4ab53e95-a9d4-4fbd-ac49-1da62557ee7f'
        url = f'{prefix}/{geoloc}/{industrial_class}/{other}/{token}'

        result = urllib.request.urlopen(url).read()
        result = list(map(self._add_point_object,json.loads(result)))

        return result;


    def _parse_cve_for_api(self,cvegeo):

        categories = [ d for d in self.cve_decoder.items() if d[1][1] <= len(cvegeo) ]
        parsed_cve = { d[0]:cvegeo[d[1][0]:d[1][1]] for d in categories }

        return parsed_cve;


    def _add_address_attrs(self):

        # Group by CVEGEO_ to avoid hitting the API multiple times:
        arr = []
        for cve,df in self.blocks.groupby('CVEGEO_'):

            cve_ = self._parse_cve_for_api(cve)

            # Hit the API
            try:
                denue_data = self._hit_api('0','5000',**cve_)
            except Exception as e:
                print(e,cve)

            # for each merchant in the group ...
            for index,row in df.iterrows():

                # Get the merchant's point
                merchant_point = row['geometry']

                def add_distance(x):
                    denue_point = x['point']
                    distance = merchant_point.distance(denue_point)
                    x['distance'] = distance
                    return x

                # Make sure the denue data has distances in relation to the merchant's point
                denue_data = list(map(add_distance,denue_data))

                # Turn data into a DataFrame and get the record with the shortest distance
                denue_data_df = pd.DataFrame(denue_data)
                min_ = denue_data_df[denue_data_df['distance'] == denue_data_df['distance'].min()]
                
                for c in min_.columns:

                    if c not in self.blocks.columns and c != 'point':
                        #print(c,min_[c])
                        try:
                            df.loc[index,c] = min_[c].tolist()[0]
                        except:
                            print('ererrr')

            arr.append(df)

        return pd.concat(arr)


