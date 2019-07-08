import os
import geopandas as gpd


class Order_2018_Data:

    def __init__(self,working_dir = 'inegi2018_data'):
        self.working_dir = working_dir
        self.files_dir = os.getcwd() + '/' + self.working_dir
        self.entidades = sorted(os.listdir(self.working_dir))

    #---------------------------------------------------------------------------
    # Returns an entidad's filetypes directory as a string
    #---------------------------------------------------------------------------
    def entidad_filetypes_dir(self,entidad):
        path = self.files_dir + '/' + entidad + '/conjunto de datos/'
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
            return self.load_entidad_file(entidad,filetype)

        filetype_list = list(map(wrapper,self.entidades))
        df = pd.concat(filetype_list)

        return df



if __name__ == '__main__':
    order_data = Order_2018_Data(working_dir='inegi2018_data_')
