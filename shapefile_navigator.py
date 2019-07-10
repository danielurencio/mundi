import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


class ShapefileNavigator:

    def __init__(self,working_dir = 'inegi2018_data'):
        self.working_dir = working_dir
        self.files_dir = os.getcwd() + '/' + self.working_dir
        self.entidades = sorted(os.listdir(self.working_dir))

    #---------------------------------------------------------------------------
    # Returns a list with all the entidades' directories
    #---------------------------------------------------------------------------
    def list_entidades(self):
        return os.listdir(self.working_dir)
 
    #---------------------------------------------------------------------------
    # Returns the entidad's path when its cve is passed
    #---------------------------------------------------------------------------
    def filter_entidad_by_cve(self,cve):
        cve = str(cve) if not isinstance(cve,str) else cve
        filtered = [ d for d in self.list_entidades() if cve in d ][0]
        return filtered

    #---------------------------------------------------------------------------
    # Returns an entidad's filetypes directory as a string
    #---------------------------------------------------------------------------
    def entidad_filetypes_dir(self,entidad):
        path = self.files_dir + '/' + entidad + '/conjunto de datos/'
        return path

    #---------------------------------------------------------------------------
    # Returns a filetypes path given a cve_ent
    #---------------------------------------------------------------------------
    def path_to_filetypes_cve_ent(self,cve_ent):
        ent = self.filter_entidad_by_cve(cve_ent)
        path = self.entidad_filetypes_dir(ent)
        return path

    #---------------------------------------------------------------------------
    # Returns a GeoDataFrame given an 'entidad' directory and a filetype name
    #---------------------------------------------------------------------------
    def load_entidad_file(self,entidad,filetype):
        file_path = self.entidad_filetypes_dir(entidad) + filetype
        gdf = gpd.read_file(file_path)
        return gdf
    
    #---------------------------------------------------------------------------
    # Returns a list of all the filetypes inside an 'entidad' directory
    #---------------------------------------------------------------------------
    def get_entidad_filetypes(self,entidad):
        filetypes = os.listdir(self.entidad_filetypes_dir(entidad))
        filetypes = sorted(filetypes)
        return filetypes

    #---------------------------------------------------------------------------
    # Returns a concatenated dataframe with a filetype for all entidades
    #---------------------------------------------------------------------------
    def concat_all_entities_filetype(self,filetype):

        def wrapper(entidad):
            return self.load_entidad_file(entidad,filetype).to_crs({'init': 'epsg:4326'}) 

        filetype_list = list(map(wrapper,self.entidades))
        df = pd.concat(filetype_list)

        return df



if __name__ == '__main__':
    lat_long = (20.580,-100.325)
    p = [Point([lat_long[1],lat_long[0]])]
    point = gpd.GeoDataFrame(p)
    point.crs = { 'init':'epsg:4326' }
    point.columns = ['geometry']

    sh_navigator = ShapefileNavigator(working_dir='inegi2018_data_')
    #ents = sh_navigator.concat_all_entities_filetype('ent')#.to_crs({'init': 'epsg:4326'})

    #example = gpd.sjoin(point,ents,op='within')
