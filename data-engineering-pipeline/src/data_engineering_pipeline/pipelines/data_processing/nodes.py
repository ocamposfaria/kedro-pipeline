import requests
import zipfile
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from typing import Any, Dict, Tuple

def download_and_unzip(parameters: Dict[str, Any])  -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    baixa e extrai os arquivos CDA de determinado mês.
    para mudar o mês, altere o parâmetro year_month.
    """
    year_month = parameters["year_month"]
    print(f"generating {year_month}")

    response = requests.get(f"https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{year_month}.zip")
    filename = f"cda_fi_{year_month}"
    
    if open(filename, "wb").write(response.content):
        print('files downloaded')

    with zipfile.ZipFile(filename, 'r') as zip_ref:
        path = r"..\..\..\data\01_raw"
        zip_ref.extractall(path)
        print('files unzipped')
    
    cda_fi_BLC_1 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_1_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_BLC_2 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_2_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_BLC_3 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_3_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_BLC_4 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_4_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_BLC_5 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_5_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_BLC_6 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_6_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_BLC_7 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_7_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_BLC_8 = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_BLC_8_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_CONFID = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_CONFID_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)
    cda_fi_PL = pd.read_csv(fr'..\..\..\data\01_raw\cda_fi_PL_{year_month}.csv', encoding="latin-1", sep = ';', low_memory=False)

    cda_fi_BLC_1['BLOCO_REP'] = 'TÍTULOS PÚBLICOS DO SELIC'
    cda_fi_BLC_2['BLOCO_REP'] = 'COTAS DE FUNDOS DE INVESTIMENTO'
    cda_fi_BLC_3['BLOCO_REP'] = 'SWAP'
    cda_fi_BLC_4['BLOCO_REP'] = 'DEMAIS ATIVOS CODIFICADOS'
    cda_fi_BLC_5['BLOCO_REP'] = 'DEPÓSITOS A PRAZO E OUTROS TÍTULOS DE IF'
    cda_fi_BLC_6['BLOCO_REP'] = 'TÍTULOS DO AGRONEGÓCIO E DE CRÉDITO PRIVADO'
    cda_fi_BLC_7['BLOCO_REP'] = 'INVESTIMENTO NO EXTERIOR'
    cda_fi_BLC_8['BLOCO_REP'] = 'DEMAIS ATIVOS NÃO CODIFICADOS'

    return cda_fi_BLC_1, cda_fi_BLC_2, cda_fi_BLC_3, cda_fi_BLC_4, cda_fi_BLC_5, cda_fi_BLC_6, cda_fi_BLC_7, cda_fi_BLC_8, cda_fi_CONFID, cda_fi_PL


def aggregate_files(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame, df4: pd.DataFrame, df5: pd.DataFrame, df6: pd.DataFrame, df7: pd.DataFrame, df8: pd.DataFrame) -> pd.DataFrame:
    """
    une os dataframes pelas suas colunas comuns (é um append/union all)
    """
    df_aggregated = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8])
    print('files aggregated')

    return df_aggregated

def one_hot_encoding_asset_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    performa OneHotEncoding nos tipos de ativos, e agrupa os dados para granularidade de fundos.
    """
    
    df = df[['CNPJ_FUNDO','DENOM_SOCIAL', 'TP_ATIVO']]
    df_groupped = df.groupby(['CNPJ_FUNDO','DENOM_SOCIAL', 'TP_ATIVO'], as_index=False).first()
    X = df_groupped
    
    # put categorical variables in a list 
    categorical_vars = ["TP_ATIVO"]
    # instantiate the one hot encoder
    one_hot_encoder = OneHotEncoder(sparse_output=False, drop = "first")
    # apply the one hot encoder logic 
    encoder_vars_array = one_hot_encoder.fit_transform(X[categorical_vars])
    # create object for the feature names using the categorical variables
    encoder_feature_names = one_hot_encoder.get_feature_names_out(categorical_vars)
    # create a dataframe to hold the one hot encoded variables
    encoder_vars_df = pd.DataFrame(encoder_vars_array, columns = encoder_feature_names)
    # concatenate the new dataframe back to the original input variables dataframe
    X_new = pd.concat([X.reset_index(drop=True), encoder_vars_df.reset_index(drop=True)], axis = 1)
    # drop the original input 2 and input 3 as it is not needed anymore
    X_new.drop(categorical_vars, axis = 1, inplace = True)

    df_one_hot = X_new.groupby(['CNPJ_FUNDO', 'DENOM_SOCIAL']).sum().reset_index()

    return df_one_hot

def prepare_to_postgresql(df: pd.DataFrame):
    df = df[['CNPJ_FUNDO', 'QT_POS_FINAL', 'VL_MERC_POS_FINAL', 'TP_ATIVO']]
    return df