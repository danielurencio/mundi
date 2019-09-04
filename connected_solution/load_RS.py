import pandas as pd, os, Cred
from sqlalchemy import create_engine

f = os.listdir('data')

cols = [
        'merchant_id',
        'CVEGEO_',
        'AGEB',
        'CP',
        'Calle',
        'Colonia',
        'EDIFICIO_PISO',
        'Manzana',
        'Num_Exterior',
        'Num_Interior',
        'Tipo',
        'Tipo_vialidad',
        'Ubicacion',
        'nom_corredor_industrial',
        'numero_local',
        'tipo_corredor_industrial'
        ]


conn_str = Cred.generate_redshift_conn_str('prod')
conn = create_engine(conn_str)

arr = []

for i in f:
    df = pd.read_csv('data/' + i)[cols]

    for c in cols:
        df[c] = df[c].astype(object)

    try:
        arr.append(df)
        #df.to_csv(f'data_for_rs/{i}',header=False)
        #df.to_sql('inegi_scrape',conn,if_exists='append',index=False,schema='clipsandbox')
    except:
        os.system(f'echo {i} >> loading_errors')

    print(i)

#pd.concat(arr).to_csv('all.csv',index=False)
