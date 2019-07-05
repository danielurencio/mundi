import os,json, re

def get_state_urls():

    # Load initial URL
    inegi_url = re.sub('\n','',open('inegi_url.txt').read())
    
    # Construct the curl command
    curl_command = f'curl {inegi_url} >> inegis_response.json'
    
    # Execute the command
    os.system(curl_command)
    
    # Open generated file, parse it as a JSON and delete response file
    inegi_json = open('inegis_response.json').read()
    inegi_json = json.loads(inegi_json)
    os.system('rm inegis_response.json')

    # Extract URL from JSON
    hijos = inegi_json['multiArchivos'][0]['hijos']
    hijos = map(lambda hijo: hijo['url'],hijos)
    state_urls = list(hijos)

    return state_urls



def download_zipped_shapefile(state_url):
    os.system(f'curl {state_url} -O')



def download_and_extract_into_named_dir(state_url):
    
    file_name = state_url.split('/')[-1]
    dir_name = file_name.split('.')[0]

    os.system(f'curl {state_url} -O >/dev/null 2>&1')
    os.system(f'mkdir {dir_name}')
    os.system(f'unzip {file_name} -d {dir_name} >/dev/null 2>&1')
    os.system(f'rm {file_name}')

    print(dir_name)


def order_directories_by_shapefile():

    state_dirs =  sorted(list(filter(lambda x:re.search('[0-9]',x),os.listdir())))

    def try_convert_toInt(x):
        try:
            result = int(x)
        except:
            result = x
        return result

    def checkInstance(x):
        char = try_convert_toInt(x)
        return isinstance(char,int)
      

    for d in state_dirs:

        shapefiles = os.listdir(f'{d}/conjunto de datos')
        file_prefixes = list(set(map(lambda x:x.split('.')[0],shapefiles)))
        file_prefixes.sort()

        for f in file_prefixes:
            cve_ent = f[:2]
            file_type = f[2:]
            starts_with_number = all([ checkInstance(d) for d in cve_ent ])
            
            directory = file_type if starts_with_number else f

            os.system(f'mkdir "{d}/conjunto de datos/{directory}"')
            os.system(f'mv "{d}/conjunto de datos"/{f}.* "{d}/conjunto de datos/{directory}"')
          



def get_all_states():
    state_urls = get_state_urls()
    list(map(download_and_extract_into_named_dir,state_urls))
    order_directories_by_shapefile()

if __name__ == '__main__':
    get_all_states()
    #order_directories_by_shapefile()
