import os, requests, json, sqlalchemy
import pandas as pd
from random import randint
from time import sleep
from zipfile import ZipFile
from glob import glob


class Denue:

    def __init__(self):
        
        self.url = 'https://www.inegi.org.mx/app/descarga/DescargaMasivaWS.asmx/ObtenerArchivosSub'
        self.file_url = 'https://www.inegi.org.mx/contenidos'

        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'www.inegi.org.mx',
            'Origin': 'https://www.inegi.org.mx',
            'Referer': 'https://www.inegi.org.mx/app/descarga/?ti=6',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        self.params = {
            'tipoInformacion': 6,
            'areaGeografica': 0,
            'proyecto': 224,
            'periodo': 0,
            'subtema': 0,
            'formato': 0,
            'periodoinfo': 629,
            'titulo': 'DENUE|Actividad económica',
            'desde': 1,
            'hasta': 10000,
            'datosAbiertos': 3,
            'filtrarTemas': 0,
            'textoBuscar': '',
            'enIngles': 0
        }


    def make_request_for_initial_urls(self):

        request = requests.get(self.url, headers=self.headers, params=self.params)
        response = request.json()

        return response



    @staticmethod
    def single_element_parser(x):

        def periodo(x):
            return x[1:] if '|' in x else x
        
        def titulo(x):
            return x.split('|')[-1]

        def extension(x):
            return x.split('&')[0]

        func_dict = {
            'Extension':extension,
            'Titulo':titulo,
            'Periodo':periodo,
            'PathLogico':lambda x:x
        }

        subset = ('Extension','PathLogico','Periodo','Titulo')
        filtered_dict = { d:func_dict[d](x[d]) for d in x if d in subset }

        return filtered_dict



    def parse_initial_urls(self):

        response = self.make_request_for_initial_urls()
        parsed_response = list(map(self.single_element_parser,response))
        
        df = pd.DataFrame(parsed_response)
        df['url_portion'] = df.apply(lambda x:x['PathLogico'] + x['Extension'],axis=1)
        df['Periodo'] = df.Periodo.map(lambda x:pd.datetime.strptime(x,'%m/%Y' if '/' in x else '%Y'))

        df = df[['Titulo','Periodo','url_portion']]


        return df
        

    def download_files(self):

        if 'denue' not in os.listdir():
            os.system('mkdir denue')

        df = self.parse_initial_urls()

        for i,d in df.iterrows():
            
            complete_url = self.file_url + d['url_portion']
            filename = complete_url.split('/')[-1]
            path = filename.split('.')[0]

            if 'path' not in os.listdir('denue'):
                os.system(f'mkdir denue/{path}')

            if filename not in os.listdir(f'denue/{path}'):

                os.system(f'curl -o denue/{path}/{filename} {complete_url}')
            
                # Force some sleep time so that INEGI doesn't close the connection.
                seconds_to_wait = randint(10,20)
                sleep(seconds_to_wait)

                print(complete_url)



    def unzip_files(self):

        error_log = []

        if 'denue' not in os.listdir():
            self.download_files()

        for d in os.listdir('denue'):

            try:
                path = f'denue/{d}'
                filename = os.listdir(path)[0]
                zip_instance = ZipFile(f'{path}/{filename}','r')
                zip_instance.extractall(f'{path}/')

            except Exception as e:
                print(e)
                error_log.append(d)
                pd.DataFrame({ 'error':error_log }).to_csv(f'denue/error_log.csv',index=False)


    @staticmethod
    def get_all_csv():
        path = 'denue'
        result = [y for x in os.walk(path) for y in glob(os.path.join(x[0], '*.csv'))]
        result = [ d for d in result if 'diccionario' not in d and 'error' not in d ]
        return result

    
    def get_columns(self):
        csvs = self.get_all_csv()
        
        cols = []
        errors = []

        for csv in csvs:

            try:
                cols.append(pd.read_csv(csv,encoding='latin1').columns.tolist())

            except Exception as e:
                print(csv,e)
                errors.append(csv)

        return cols,errors




    def map_columns_to_file(self,**kwargs):

        if not kwargs:
            cols, errors = self.get_columns()
        else:
            if ('cols' not in kwargs or 'errors' not in kwargs):
                raise ValueError('You should name your key word arguments as "cols" and "errors" '+ \
                                    'for columns and errors respectively.')

            cols = kwargs['cols']
            errors = kwargs['errors']

        if not len(errors):
            files = self.get_all_csv()
            
            return list(zip(files,cols))

        else:
            raise ValueError('There were errors while parsing the files.')




    @staticmethod
    def get_column_names_homologator(cols):

        arr = [ list(set(map(lambda x:x[i],cols))) for i in range(41) ]
        arr_0 = list(filter(lambda x:len(x) == 2,arr))
        arr_1 = list(filter(lambda x:len(x) > 2,arr))

        dic = { [ i for i in d if str.isupper(i[0])][0]:[ i for i in d if str.islower(i[0]) ][0] for d in arr_0 }

        for e in arr_1[0]:
            dic[e] = 'fecha_alta'

        return dic


    def save_column_name_homologator(self):
        cols,errors = self.get_columns()
        homologator = self.get_column_names_homologator(cols)

        with open('homologator_file.json','w') as output:
            json.dump(homologator,output)
            print('An homologator JSON file has been generated to change the column names of each CSV file.')


    @staticmethod
    def fix_df_encoding_problems(df_):

        df = df_.copy()
        dtypes = df.dtypes.apply(lambda x:x.name).to_dict()
        object_types = [ d for d in dtypes if dtypes[d] == 'object' ]

        col_fn = lambda x:str(x).encode('latin1').decode('utf-8') if not pd.isnull(x) else None

        for c in object_types:
            df[c] = df[c].map(col_fn)

        return df


    def save_to_db(self,conn,dtype_dict):
        
        if 'homologator_file.json' not in os.listdir():
            self.save_column_name_homologator()

        homologator = json.loads(open('homologator_file.json').read())
        csvs = self.get_all_csv()

        csvs[0]

        errors = []

        for i,csv in enumerate(csvs):

            try:
                df = pd.read_csv(csv,encoding='latin1',low_memory=False)\
                       .rename(columns=homologator)\
                       .iloc[:,:41]

                df = self.fix_df_encoding_problems(df)
                new_t = { d:'float64' if d in ('latitud','longitud') else 'object' for d in df.columns.tolist() }
                df = df.astype(new_t)

                df.to_sql('denue',conn, if_exists='append',dtype=dtype_dict,index=False)
                print(csv)

            except Exception as e:
                print(csv,e)
                errors.append(csv)

        return errors


if __name__ == '__main__':
    denue = Denue()
    conn = sqlalchemy.create_engine('postgres://danielurencio:123123@localhost:5432/danielurencio')
    cols = ['id', 'nom_estab', 'raz_social', 'codigo_act', 'nombre_act', 'per_ocu', 'tipo_vial', 'nom_vial', 'tipo_v_e_1', 'nom_v_e_1', 'tipo_v_e_2', 'nom_v_e_2', 'tipo_v_e_3', 'nom_v_e_3', 'numero_ext', 'letra_ext', 'edificio', 'edificio_e', 'numero_int', 'letra_int', 'tipo_asent', 'nomb_asent', 'tipoCenCom', 'nom_CenCom', 'num_local', 'cod_postal', 'cve_ent', 'entidad', 'cve_mun', 'municipio', 'cve_loc', 'localidad', 'ageb', 'manzana', 'telefono', 'correoelec', 'www', 'tipoUniEco', 'latitud', 'longitud', 'fecha_alta']
    dtype_dict = { d:sqlalchemy.dialects.postgresql.VARCHAR if d not in ('latitud','longitud') else sqlalchemy.dialects.postgresql.FLOAT for d in cols }
    #denue.download_files()
