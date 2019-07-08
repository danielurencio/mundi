import requests, re, pandas as pd, os
from unidecode import unidecode
from pprint import pprint
from random import randint
from time import sleep
from zipfile import ZipFile

#-------------------------------------------------------------------------
# ------------------------------------------------------------------------

class Inegi2010:

    def __init__(self,initial_page=0):

        # The 'initial page' parameter indicates from what page this code should begin extracting URLs.
        self.initial_page = initial_page
        self.arr = []

        self.url = 'https://www.inegi.org.mx/app/api/productos/interna_v1/slcCartas/obtenCartas'

        self.headers = {
            'Accept': "application/json, text/javascript, */*; q=0.01",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "en-US,en;q=0.9",
            'Connection': 'keep-alive',
            'Host': "www.inegi.org.mx",
            'Referer': "https://www.inegi.org.mx/app/mapas/",
            'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
            'X-Requested-With': "XMLHttpRequest"
        }

        self.params = {
            'enti':'' ,
            'muni':'',
            'loca':'',
            'tema':'',
            'titg':'',
            'esca':'',
            'edic':'',
            'form':'',
            'busc':'números exteriores',
            'adv':'false',
            'rango':'',
            'sens':'',
            'uedo':'',
            'reso':'',
            'point':'',
            'tipoB': '1',
            'orden': '4',
            'pagi': 0,
            'tama': '100',
            'desc': 'true'
            #'_': 1562337934255
        }


    #|----------------------------------------------------------------------------------------------|


    @staticmethod
    def refine_info_strings(key,value):
        # Try refining strings: shorten the title, and trim the URL
        try:
            if key == 'titulo':
                splitted_val = value.split('. ')
                value_to_return = splitted_val[0] + ' - ' + splitted_val[-1]

            elif key == 'entidad':
                value_to_return = value

            elif key == 'formatos':
                splitted_val = value.split(' ')
                value_to_return = [ d for d in splitted_val if 'http' in d ][0]
                
                for i in ('href=','"'):
                    value_to_return = re.sub(i,'',value_to_return)

        except Exception as e:
            print(e)

        return value_to_return

    #|----------------------------------------------------------------------------------------------|


    def process_batch(self,response):

        # Filter title, state and URL
        desired_keys = ('titulo','entidad','formatos') 
        filter_keys = lambda x: { k: self.refine_info_strings(k,x[k]) for k in x if k in desired_keys }
        current_batch = list(map(filter_keys, response))
        
        return current_batch

    #|----------------------------------------------------------------------------------------------|

    def request_outcome(self,page_num):
        # Try to make a GET request, if something errors out print the last attempted page number.
        try:
            # Ensure the page number parameter is a string.
            self.params['pagi'] = page_num#str(page_num) if not isinstance(page_num,str) else page_num

            request = requests.get(self.url,headers=self.headers,params=self.params)
            response = request.json()

            return response

        except Exception as e:
            print(e)
            print(self.headers['pagi'])
            return


    #|----------------------------------------------------------------------------------------------|

    def get_all_possible_batches(self):

        page_num = self.initial_page
        print("Getting files' URLs. This might take a while...")

        while True:
            # Force some sleep time so that INEGI doesn't close the connection.
            seconds_to_wait = randint(10,20)
            sleep(seconds_to_wait)
            # Then make the request.
            response = self.request_outcome(page_num)

            condition = True if 'mapas' in response.keys() else False

            # If the status code is 200, keep getting batches.
            if condition:

                if(page_num % 1 == 0 and page_num != 0):
                    print('Page #: ',self.params['pagi'])

                response = response['mapas']
                current_batch = self.process_batch(response)
                self.arr += current_batch
                page_num += 1

            # Else, we are done!
            else:
                print('finished!')
                break
    
    #|----------------------------------------------------------------------------------------------|

    def save_url_file(self):
        self.get_all_possible_batches()
        df = pd.DataFrame(self.arr)
        df['municipio'] = df.titulo.map(lambda x:x.split(' - ')[-1])
        df['titulo'] = df.titulo.map(lambda x:x.split(' - ')[0])
        df['region'] = df.titulo.map(lambda x:x.split(', ')[-1])
        df.drop('titulo',axis=1,inplace=True)
        df.to_csv('urls.csv',index=False)
        
    #|----------------------------------------------------------------------------------------------|

    @staticmethod
    def parse_urls_and_filenames():

        if not 'urls.csv' in os.listdir():
            self.save_url_file()
            df = pd.read_csv('urls.csv')

        else:
            df = pd.read_csv('urls.csv')

        def correct_name(x):
            name = unidecode(x).upper()
            name = re.sub('^ ','',name)
            name = re.sub(' ','_',name)
            name = re.sub('}','',name)
            name = re.sub(' -','',name)
            return name

        df['entidad'] = df['entidad'].map(correct_name)
        df['municipio'] = df['municipio'].map(correct_name)
        df = df.sort_values(['entidad','municipio'],ascending=[True, True])

        df['filename'] = df.apply(lambda x:'__'.join([x.entidad,x.municipio,x.region,'.zip']),axis=1)
        df = df[['filename','formatos']].rename(columns={ 'formatos':'url' })

        return df

    #|----------------------------------------------------------------------------------------------|

    def download_all_files(self):

        self.download_list = self.parse_urls_and_filenames()

        if 'num_ext' not in os.listdir():
            os.system('mkdir num_ext')

        for i,d in self.download_list.iterrows():

            if d.filename in os.listdir('num_ext'):
                filename = d.filename.split('.')
                filename = filename[0] + '_1.' + filename[-1]

            else:
                filename = d.filename
            
            # Force some sleep time so that INEGI doesn't close the connection.
            seconds_to_wait = randint(10,20)
            sleep(seconds_to_wait)

            print('-------------------------------------------------------------------------------')
            print(f'Downloading {filename}..')
            os.system(f'curl -o num_ext/{filename} {d.url}')
            print('-------------------------------------------------------------------------------')
            print('')

            try:
                zip_instance = ZipFile(f'num_ext/{filename}','r')
                dir_to_extract = 'num_ext/' + filename.split('.')[0]
                os.system(f'mkdir {dir_to_extract}')
                zip_instance.extractall(f'{dir_to_extract}')
                os.system(f'rm num_ext/{filename}')

            except:
                ix = self.download_list[self.download_list['filename'] == d.filename].index
                self.download_list.loc[ix,'error'] = 1
                self.download_list[self.download_list.error == 1].to_csv('error_log.csv',index=False)

            
    #|----------------------------------------------------------------------------------------------|

if __name__ == '__main__':
    inegi = Inegi2010(0) 
    #inegi.download_all_files()
