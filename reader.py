import PyPDF2
import re

def extrair_campos_pdf_pypdf2(caminho_pdf):
    """Extrai campos específicos do PDF usando PyPDF2 + regex"""
    
    try:
        # Ler o PDF com PyPDF2
        with open(caminho_pdf, 'rb') as arquivo:
            leitor_pdf = PyPDF2.PdfReader(arquivo)
            
            texto_completo = ""
            for pagina in leitor_pdf.pages:
                texto_pagina = pagina.extract_text()
                texto_completo += texto_pagina + "\n"
        
        # Definir padrões regex para cada campo
        padroes = {
            "ano": r"Ano:\s*(\d{4})",
            "trimestre": r"Trimestre:\s*(\d+º?\s*Trimestre)",
            "valor_pedido": r"Valor do Pedido:\s*([\d.,]+)",
            "cnpj": r"CNPJ:\s*(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})",
            "nome_empresarial": r"Nome Empresarial:\s*(.+?)(?=\n|$)",
            "data_transmissao": r"Data de Transmissão:\s*(\d{2}/\d{2}/\d{4})",
            "numero_controle": r"Número de Controle:\s*([^\n]+)"
        }
        
        # Extrair campos usando regex
        campos_extraidos = {}
        for campo, padrao in padroes.items():
            match = re.search(padrao, texto_completo, re.MULTILINE | re.IGNORECASE)
            if match:
                campos_extraidos[campo] = match.group(1).strip()
            else:
                campos_extraidos[campo] = None

        campos_extraidos["numero_perdcomp"] = caminho_pdf.split('/')[-1].rstrip('.pdf').replace('recibo_', '')        
        campos_extraidos['tipo_credito'] = caminho_pdf.split('/')[-2].capitalize()
        return campos_extraidos
        
    except Exception as e:
        print(f"Erro ao processar PDF {caminho_pdf}: {e}")
        return {}

# Exemplo de uso:
def testar_extracao_pdf():
    caminho_pdf = "/home/vitorio/Downloads/gestao1273/cnpjs/32065029000148/compensacao/recibo_008586319730102513191047.pdf"
    
    campos = extrair_campos_pdf_pypdf2(caminho_pdf)

    print(campos)
    # 
    # print("Campos extraídos:")
    # print(f"Ano: {campos.get('ano')}")
    # print(f"Trimestre: {campos.get('trimestre')}")
    # print(f"Valor do Pedido: {campos.get('valor_pedido')}")
    # print(f"CNPJ: {campos.get('cnpj')}")
    # print(f"Data de Transmissão: {campos.get('data_transmissao')}")
    # print(f"Número do Documento: {campos.get('numero_documento')}")
    
if __name__ == "__main__":
    testar_extracao_pdf()