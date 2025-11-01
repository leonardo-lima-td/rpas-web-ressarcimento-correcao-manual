import pandas as pd
import psycopg2
import boto3
import pathlib as p
import os
from dotenv import load_dotenv

load_dotenv()

def update_database(query):
    """Atualiza registros no banco PostgreSQL"""
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST_PRD'),
        port=os.getenv('DB_PORT'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        print(f"Atualizados 1 registro no banco")
    except Exception as e:
        print(f"Erro ao atualizar banco: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def upload_pdfs(file_path, s3_key):
    """Faz upload dos PDFs para S3"""
    uploaded = 0
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    try:
        s3_client.upload_file(file_path, os.getenv('AWS_S3_BUCKET'), s3_key)
        print(f"Upload: {file_path} para {s3_key}")
        uploaded += 1
    except Exception as e:
        print(f"Erro upload {file_path}: {e}")

    print(f"Total PDFs enviados: {uploaded}")

def renomear_arquivo(caminho_antigo, caminho_novo):
    try:
        arquivo_antigo = p.Path(caminho_antigo)
        arquivo_novo = p.Path(caminho_novo)
        
        if not arquivo_antigo.exists():
            print(f"Erro: Arquivo {caminho_antigo} não encontrado")
            return False
        
        arquivo_antigo.rename(arquivo_novo)
        print(f"Arquivo renomeado: {caminho_antigo} -> {caminho_novo}")
        return True
        
    except Exception as e:
        print(f"Erro ao renomear arquivo: {e}")
        return False


def main():
    """Função principal"""
    # Lê CSV

    df = pd.read_csv('gest1273.csv', sep=',', encoding='utf-8', dtype=str)

    for index, row in df.iterrows():
        id = row.iloc[0]
        status = "sucesso"
        cpf = row.iloc[1]
        perdcomp = row.iloc[2]
        path_s3 = f'25084515000148/pedidos/{cpf}/ressarcimento/'
        path_s3_recibo = path_s3 + f'recibo_{perdcomp}.pdf'
        path_s3_detalhamento = path_s3 + f'{perdcomp}.pdf'

        query = f"UPDATE public.creditos SET status_pedido='{status}', perdcomp='{perdcomp}', path_s3_recibo='{path_s3_recibo}', path_s3_detalhamento='{path_s3_detalhamento}' WHERE id_pedido={id};"

        print(query)
        path_local_recibo = p.Path(__file__).parent.joinpath('recibo_' + perdcomp + '.pdf').absolute()
        path_local_detalhamento = p.Path(__file__).parent.joinpath(perdcomp + '.pdf').absolute()

        print(path_local_recibo)
        print(path_local_detalhamento)

        upload_pdfs(path_local_recibo, path_s3_recibo)
        upload_pdfs(path_local_detalhamento, path_s3_detalhamento)

        update_database(query)

    print("Processo concluído!")

if __name__ == "__main__":
    main()
