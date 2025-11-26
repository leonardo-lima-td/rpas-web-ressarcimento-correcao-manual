import pandas as pd
import psycopg2
import boto3
import pathlib as p
import os
import subprocess
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
    except Exception as e:
        print(f"Erro ao atualizar banco: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def executar_query_pandas(query):
    """Executa query SQL e retorna DataFrame pandas"""
    
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST_PRD'),
        port=os.getenv('DB_PORT'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    
    try:
        # Executa query e converte diretamente para DataFrame
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Obter nomes das colunas
        colunas = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Obter resultados
        resultados = cursor.fetchall()
        
        # Converter para DataFrame
        df_resultado = pd.DataFrame(resultados, columns=colunas)
        return df_resultado
    except Exception as e:
        print(f"Erro ao executar query: {e}")
        return pd.DataFrame()  # Retorna DataFrame vazio em caso de erro
    finally:
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

    df = pd.read_csv('perdcomps.csv', sep=',', encoding='utf-8', dtype=str)

    for index, row in df.iterrows():
        id = row.iloc[0]
        status = "sucesso"
        cpf = row.iloc[2]
        perdcomp = row.iloc[1]
        path_s3 = f'25084515000148/pedidos/{cpf}/ressarcimento/'
        path_s3_recibo = path_s3 + f'recibo_{perdcomp}.pdf'
        path_s3_detalhamento = path_s3 + f'{perdcomp}.pdf'

        query = f"UPDATE public.creditos SET status_pedido='{status}', perdcomp='{perdcomp}', path_s3_recibo='{path_s3_recibo}', path_s3_detalhamento='{path_s3_detalhamento}' WHERE id_pedido={id};"

        print(query)
        path_local_recibo = p.Path(__file__).parent.joinpath('data_pdfs/recibo_' + perdcomp + '.pdf').absolute()
        path_local_detalhamento = p.Path(__file__).parent.joinpath('data_pdfs/' + perdcomp + '.pdf').absolute()

        print(path_local_recibo)
        print(path_local_detalhamento)

        # path_local_recibo_renomeado = path_local_recibo.replace('data_pdfs/recibo-perdcomp_', 'data_pdfs/recibo_')

        # renomear_arquivo(path_local_recibo, path_local_recibo_renomeado)

        upload_pdfs(path_local_recibo, path_s3_recibo)
        upload_pdfs(path_local_detalhamento, path_s3_detalhamento)

        update_database(query)


    print("Processo concluído!")

# def listar_pastas_s3(s3_prefix):
#     """Lista apenas os nomes das pastas em um prefixo do S3"""
#     s3_client = boto3.client(
#         's3',
#         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
#         region_name=os.getenv('AWS_REGION')
#     )
    
#     bucket = os.getenv('AWS_S3_BUCKET')
    
#     # Usar um set para evitar duplicatas
#     pastas = set()
    
#     paginator = s3_client.get_paginator('list_objects_v2')
#     pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix, Delimiter='/')
    
#     for page in pages:
#         # CommonPrefixes contém as "pastas" no nível atual
#         if 'CommonPrefixes' in page:
#             for prefix in page['CommonPrefixes']:
#                 # Remove o prefixo base e a barra final para obter apenas o nome da pasta
#                 nome_pasta = prefix['Prefix'].replace(s3_prefix, '').rstrip('/')
#                 pastas.add(nome_pasta)
    
#     return list(pastas)

# def download_pasta_completa(s3_prefix_pasta, local_dir):
#     """Download completo de uma pasta específica do S3"""
#     s3_client = boto3.client(
#         's3',
#         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
#         region_name=os.getenv('AWS_REGION')
#     )
    
#     bucket = os.getenv('AWS_S3_BUCKET')
    
#     # Cria diretório local
#     p.Path(local_dir).mkdir(parents=True, exist_ok=True)
    
#     # Lista todos os objetos dentro da pasta (incluindo subpastas)
#     paginator = s3_client.get_paginator('list_objects_v2')
#     pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix_pasta)
    
#     downloaded = 0
#     for page in pages:
#         if 'Contents' in page:
#             for obj in page['Contents']:
#                 s3_key = obj['Key']
                
#                 # Caminho relativo removendo o prefixo da pasta
#                 relative_path = s3_key.replace(s3_prefix_pasta, '', 1)
#                 local_path = p.Path(local_dir) / relative_path
                
#                 # Cria subdiretórios
#                 local_path.parent.mkdir(parents=True, exist_ok=True)
                
#                 try:
#                     s3_client.download_file(bucket, s3_key, str(local_path))
#                     print(f"✓ {s3_key}")
#                     downloaded += 1
#                 except Exception as e:
#                     print(f"✗ Erro: {s3_key} - {e}")
    
#     return downloaded


# def listar_arquivos_pdf_sem_recibo(diretorio_base):
#     # Converte para Path
#     json_campos = []
#     base_path = p.Path(diretorio_base)
    
#     # Percorre recursivamente todos os arquivos
#     for arquivo_path in base_path.rglob('*.pdf'):
#         nome_arquivo = arquivo_path.name
        
#         # Verifica as condições: não começa com 'recibo' e termina com '.pdf'
#         if nome_arquivo.startswith('recibo') and nome_arquivo.endswith('.pdf'):
#             from reader import extrair_campos_pdf_pypdf2
#             campos = extrair_campos_pdf_pypdf2(str(arquivo_path))
#             json_campos.append(campos)

#     return json_campos

# # def run_listar_arquivos_pdf_sem_recibo():
#     diretorio = "/home/vitorio/Downloads/gestao1273/cnpjs"
#     json_campos = listar_arquivos_pdf_sem_recibo(diretorio)
    
#     print(f"Encontrados {len(json_campos)} PDFs com dados extraídos")
    
#     resultados_finais = []
    
#     for dados_pdf in json_campos:
#         perdcomp = dados_pdf.get('numero_perdcomp')
        
#         if not perdcomp:
#             print(f"⚠️  PDF sem número PERDCOMP: {dados_pdf.get('caminho_arquivo', 'desconhecido')}")
#             continue
        
#         print(f"\n🔍 Verificando PERDCOMP: {perdcomp}")
        
#         # Query para buscar dados do banco
#         query = f"""
#         SELECT 
#             c.perdcomp,
#             true as arquivo_existe_no_banco,
#             c.id_pedido,
#             c.gestao_creditos_id,
#             c.status_pedido,
#             c.path_s3_recibo,
#             c.path_s3_detalhamento
#         FROM 
#             public.creditos c 
#         WHERE 
#             c.perdcomp = '{perdcomp}';
#         """
        
#         df_banco = executar_query_pandas(query)
        
#         if True:
#             # Não encontrou no banco - criar linha com dados do PDF + campos vazios
#             print("❌ Não encontrado no banco")
            
#             linha_combinada = {
#                 # Dados do PDF
#                 'numero_perdcomp': dados_pdf.get('numero_perdcomp'),
#                 'ano': dados_pdf.get('ano'),
#                 'trimestre': dados_pdf.get('trimestre'),
#                 'valor_pedido': dados_pdf.get('valor_pedido'),
#                 'cnpj': dados_pdf.get('cnpj'),
#                 'nome_empresarial': dados_pdf.get('nome_empresarial'),
#                 'data_transmissao': dados_pdf.get('data_transmissao'),
#                 'numero_documento': dados_pdf.get('numero_documento'),
#                 'numero_controle': dados_pdf.get('numero_controle'),
#                 'tipo_credito': dados_pdf.get('tipo_credito'),
#                 'caminho_arquivo': dados_pdf.get('caminho_arquivo'),
                
#                 # Campos do banco (vazios)
#                 'arquivo_existe_no_banco': False,
#                 'id_pedido': None,
#                 'gestao_creditos_id': None,
#                 'status_pedido': None,
#                 'path_s3_recibo': None,
#                 'path_s3_detalhamento': None
#             }
#         else:
#             # Encontrou no banco - mesclar dados
#             print("✅ Encontrado no banco")
            
#             dados_banco = df_banco.iloc[0].to_dict()
            
#             linha_combinada = {
#                 # Dados do PDF
#                 'numero_perdcomp': dados_pdf.get('numero_perdcomp'),
#                 'ano': dados_pdf.get('ano'),
#                 'trimestre': dados_pdf.get('trimestre'),
#                 'valor_pedido': dados_pdf.get('valor_pedido'),
#                 'cnpj': dados_pdf.get('cnpj'),
#                 'nome_empresarial': dados_pdf.get('nome_empresarial'),
#                 'data_transmissao': dados_pdf.get('data_transmissao'),
#                 'numero_documento': dados_pdf.get('numero_documento'),
#                 'numero_controle': dados_pdf.get('numero_controle'),
#                 'tipo_credito': dados_pdf.get('tipo_credito'),
#                 'caminho_arquivo': dados_pdf.get('caminho_arquivo'),
                
#                 # Dados do banco
#                 'arquivo_existe_no_banco': dados_banco.get('arquivo_existe_no_banco'),
#                 'id_pedido': dados_banco.get('id_pedido'),
#                 'gestao_creditos_id': dados_banco.get('gestao_creditos_id'),
#                 'status_pedido': dados_banco.get('status_pedido'),
#                 'path_s3_recibo': dados_banco.get('path_s3_recibo'),
#                 'path_s3_detalhamento': dados_banco.get('path_s3_detalhamento')
#             }
        
#         resultados_finais.append(linha_combinada)
    
#     # Converter para DataFrame para facilitar manipulação
#     df_final = pd.DataFrame(resultados_finais)
    
#     # Salvar em CSV
#     arquivo_saida = 'dados_pdf_banco_combinados.csv'
#     df_final.to_csv(arquivo_saida, index=False)
#     print(f"\n💾 Dados salvos em '{arquivo_saida}'")
    
#     # Estatísticas
#     total = len(df_final)
#     no_banco = df_final['arquivo_existe_no_banco'].sum()
#     fora_banco = total - no_banco
    
#     print("📊 Estatísticas:")
#     print(f"   Total de PDFs processados: {total}")
#     print(f"   Encontrados no banco: {int(no_banco)}")
#     print(f"   Não encontrados no banco: {int(fora_banco)}")
    
#     return df_final



# Uso:
if __name__ == "__main__":
    main()
    # import json
    
    # df_resultados = run_listar_arquivos_pdf_sem_recibo()
    
    # # Mostrar primeiras linhas
    # if not df_resultados.empty:
    #     print("\n🔍 Primeiras linhas do resultado:")
    #     print(df_resultados.head().to_string(index=False))
