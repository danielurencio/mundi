import requests, re, pandas as pd, os
from pprint import pprint
from random import randint
from time import sleep

class Inegi2010:

    def __init__(self,initial_page=0):
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

        if not 'urls.csv' in os.listdir():
            self.save_url_file()

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
        pd.DataFrame(self.arr).to_csv('urls.csv',index=False)
        
    #|----------------------------------------------------------------------------------------------|


if __name__ == '__main__':
    inegi = Inegi2010(0)
    #inegi.save_url_file()
