import os,requests
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
        result = [ d for d in result if d not in ('diccionario','error') ]
        return result

    
    def check_columns(self):
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


if __name__ == '__main__':
    denue = Denue()
    #denue.download_files()
