import pandas as pd
import sqlite3 


# Fase 1 - Extração
def extract_data(file_path):
    df = pd.read_csv(file_path)
    print("✅ Dados extraidos com sucesso!")
    return df


# Fase 2 - Transformação
def transform_data(df):
    df['valor_total'] = df['quantidade'] * df['preco_unitario']
    df['data_venda'] = pd.to_datetime(df['data_venda'])
    
    resumo_clientes = df.groupby('cliente')['valor_total'].sum().reset_index()
    resumo_clientes.rename(columns={'valor_total': 'total_vendas_clientes'}, inplace=True)
    
    print("✅ Dados transformados com sucesso!")
    return df, resumo_clientes


# Fase 3 - Carga
def load_data(df, resumo_clientes, db_path):
    conn = sqlite3.connect(db_path)
    df.to_sql('vendas', conn, if_exists='replace', index=False)
    resumo_clientes.to_sql('resumo_clientes', conn, if_exists='replace', index=False)
    conn.close()
    print("✅ Dados carregados no banco com sucesso!")
    
if __name__== "__main__":
    input_file = './data/vendas.csv'
    db_output = './output/vendas.db'
    
    df = extract_data(input_file)
    df_tratado, resumo = transform_data(df)
    load_data(df_tratado, resumo, db_output)
    
    df_tratado.to_csv('./output/vendas_tratadas.csv', index=False)
    resumo.to_csv('./output/resumo_clientes.csv', index=False)
    
    print("🏁 ETL Finalizado")