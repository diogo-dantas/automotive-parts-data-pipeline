import pandas as pd
import random
import numpy as np
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

# Criar diretórios necessários, se não existirem
Path("../data/raw").mkdir(parents=True, exist_ok=True)

# Constantes e valores de referência
TOTAL_PECAS = 2000
TOTAL_ESTOQUE = 1000
TOTAL_EXPORTACOES = 500

# Funções auxiliares
def data_mal_formatada():
    """Gera uma data com formatos aleatórios."""
    base_date = datetime(2021, 1, 1) + timedelta(days=random.randint(0, 1000))
    formatos = ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", "%Y.%m.%d"]
    return base_date.strftime(random.choice(formatos))

def preco_variado():
    """Gera preço com variações de tipo."""
    val = round(random.uniform(10, 250), 2)
    formato = random.choice(["normal", "usd", "com_cifrao", "nan"])
    if formato == "normal":
        return val
    elif formato == "usd":
        return f"USD {val}"
    elif formato == "com_cifrao":
        return f"${val}"
    else:
        return np.nan if random.random() < 0.01 else val

def descricao_suja(descricoes_base):
    """Suja a descrição."""
    desc = random.choice(descricoes_base)
    ruido = random.choice(["", " ", "!!", "***", "   ", " (NOVO)", ""])
    return f"{ruido}{desc}{ruido}".strip()

def data_inconsistente():
    """Gera uma data em formato inconsistente."""
    formatos = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%d %b %Y"]
    return (datetime.today() - timedelta(days=random.randint(0, 30))).strftime(random.choice(formatos))

def estoque_inconsistente():
    """Gera um valor de estoque em formato inconsistente."""
    formatos = ["{} unid", "{}", "{} peças", "None"]
    val = random.randint(0, 500)
    return random.choice(formatos).format(val)

def gerar_data_exportacao():
    """Gera uma data de exportação nos últimos 90 dias."""
    dias_atras = random.randint(1, 90)
    return (datetime.today() - timedelta(days=dias_atras)).strftime("%Y-%m-%d")

def data_atualizacao_random():
    """Gera uma data de atualização nos últimos 30 dias."""
    return (datetime.today() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")

# 1. Gerar tabela de peças (CSV)
def gerar_catalogo_pecas():
    """Gera o catálogo de peças e exporta para CSV."""
    print("Gerando catálogo de peças...")
    
    # Definir constantes
    descricoes_base = [
        "Pastilha de Freio Dianteira Cerâmica",
        "Filtro de Óleo AC Delco PF2257G",
        "Amortecedor Traseiro Hidráulico",
        "Bomba de Combustível Elétrica 12V",
        "Correia Dentada Poli-V 6PK",
        "Sensor de Temperatura do Motor",
        "Disco de Freio Ventilado",
        "Coxim do Motor Lado Direito",
        "Vela de Ignição Iridium",
        "Radiador de Alumínio com Reservatório"
    ]

    categorias_sujas = [
        "Freio", "freio", "FREIO", "Freios", "freios",
        "Motor", "Suspensão", "Combustível", "Transmissão", "Elétrico", "Ignição", "Arrefecimento"
    ]

    fornecedores = ["AC Delco", "Bosch", "Monroe", "Magneti Marelli", "Gates", "Delphi", "Fremax", "SKF", "NGK", "Valeo", None, ""]

    modelos_sujos = [
        "Onix; Prisma; Celta",
        "S10, Spin, Trailblazer",
        "Cobalt; Spin",
        "Cruze, Astra, Vectra",
        "Corsa, Montana, Corsa",
        "Tracker Spin",
        "Camaro, Omega",
        "Corsa, Meriva",
        "Montana,Onix",
        "Cruze; Astra"
    ]

    # Gerar IDs únicos de peças
    global id_pecas
    id_pecas = [random.randint(1000, 9999) for _ in range(TOTAL_PECAS)]
    
    # Garantir que não há duplicatas nos IDs
    id_pecas = list(set(id_pecas))
    while len(id_pecas) < TOTAL_PECAS:
        id_pecas.append(random.randint(1000, 9999))
        id_pecas = list(set(id_pecas))
    
    # Limitar ao tamanho exato necessário
    id_pecas = id_pecas[:TOTAL_PECAS]

    # Criação do DataFrame com "sujeiras"
    data = {
        "ID_Peca": id_pecas,
        "Descricao": [descricao_suja(descricoes_base) for _ in range(TOTAL_PECAS)],
        "Categoria": [random.choice(categorias_sujas) for _ in range(TOTAL_PECAS)],
        "Modelos_Compatíveis": [random.choice(modelos_sujos) for _ in range(TOTAL_PECAS)],
        "Preço_USD": [preco_variado() for _ in range(TOTAL_PECAS)],
        "Fornecedor": [random.choice(fornecedores) for _ in range(TOTAL_PECAS)],
        "Data_Lancamento": [data_mal_formatada() for _ in range(TOTAL_PECAS)],
        "Quantidade_Minima_Compra": [random.choice([1, 2, 5, 10, 20]) for _ in range(TOTAL_PECAS)],
        "Peso_kg": [round(random.uniform(0.2, 7.5), 2) for _ in range(TOTAL_PECAS)]
    }

    df_pecas_sujas = pd.DataFrame(data)
    output_path = "../data/raw/pecas_2024-07-01.csv"
    df_pecas_sujas.to_csv(output_path, index=False)
    print(f"Catálogo de peças gerado com sucesso: {output_path}")
    
    return id_pecas

# 2. Gerar tabela de estoque (JSON)
def gerar_estoque(id_pecas):
    """Gera a tabela de estoque e exporta para JSON."""
    print("Gerando dados de estoque...")
    
    # Centros de distribuição realistas
    centros_distribuicao = [
        "CD São Paulo - BR",
        "CD Gravataí - BR",
        "CD Detroit - US",
        "CD Arlington - US",
        "CD Ramos Arizpe - MX",
        "CD Silao - MX"
    ]
    
    centros_variacoes = [
        "CD SP - BR", "cd são paulo - br", "CD Gravatai - BR", "CD - SP",
        "Sao Paulo CD", "CD Detroit - US", "cd detroit", "CD ARLINGTON",
        "CD RAMOS ARIZPE - MX", "ramos arizpe cd", "CD Siloa - MX"
    ]

    # Selecionar aleatoriamente IDs de peças para o estoque
    estoque_ids = random.sample(id_pecas, min(TOTAL_ESTOQUE, len(id_pecas)))
    
    estoque_data = []
    for id_peca in estoque_ids:
        item = {
            "ID_Peca": id_peca,
            "Estoque_Brasil": estoque_inconsistente(),
            "Estoque_EUA": random.choice([estoque_inconsistente(), None, ""]),
            "Estoque_Mexico": random.choice([estoque_inconsistente(), None, ""]),
            "Data_Atualizacao": data_inconsistente(),
            "Centro_Distribuicao": random.choice(centros_variacoes)
        }
        estoque_data.append(item)

    output_path = "../data/raw/estoque_2024-07-01.json"
    with open(output_path, "w", encoding='utf-8') as f:
        json.dump(estoque_data, f, ensure_ascii=False, indent=2)
    print(f"Dados de estoque gerados com sucesso: {output_path}")
    
    return estoque_ids

# 3. Gerar tabela de exportações (Parquet)
def gerar_exportacoes(id_pecas):
    """Gera dados de exportações e exporta para Parquet."""
    print("Gerando dados de exportações...")
    
    # Selecionar aleatoriamente IDs de peças para exportações
    export_ids = random.sample(id_pecas, min(TOTAL_EXPORTACOES, len(id_pecas)))
    
    # Países destino relevantes
    destinos = ["Argentina", "Chile", "Colômbia", "México", "Estados Unidos", "Canadá", "Alemanha", "África do Sul"]

    # Códigos NCM reais ou simulados
    codigos_ncm = [
        "8708.30.90",  # Freios e suas partes
        "8409.99.99",  # Partes de motor
        "8708.80.00",  # Suspensão
        "8708.50.99",  # Eixos e partes
        "8407.34.90",  # Motores de combustão
        "8511.50.00",  # Sistemas de ignição
        "8708.70.90",  # Rodas e acessórios
        "8708.99.90"   # Outras partes e acessórios
    ]
    
    # Modos de transporte realistas
    modos_transporte = ["Marítimo", "Rodoviário", "Aéreo", "Ferroviário"]
    
    # Quantidade exportada
    quantidade_exportada = [random.randint(1, 500) for _ in range(TOTAL_EXPORTACOES)]
    
    # Construção do DataFrame com os dados simulados
    df = pd.DataFrame({
        "id_peca_export": export_ids,
        "destino": [random.choice(destinos) for _ in range(TOTAL_EXPORTACOES)],
        "quantidade_exportada": quantidade_exportada,
        "data_exportacao": [gerar_data_exportacao() for _ in range(TOTAL_EXPORTACOES)],
        "ncm_exportado": [random.choice(codigos_ncm) for _ in range(TOTAL_EXPORTACOES)],
        "peso_total_kg": [round(qty * random.uniform(0.2, 5.0), 2) for qty in quantidade_exportada],
        "valor_total_exportado": [round(qty * random.uniform(10, 300), 2) for qty in quantidade_exportada],
        "modo_transporte": [random.choice(modos_transporte) for _ in range(TOTAL_EXPORTACOES)]
    })
    
    # 1. Datas em formatos variados - agora mantendo como strings
    for i in range(0, len(df), 20):
        if i < len(df):
            # Alteramos o formato, mas mantemos como string
            try:
                data_str = df.at[i, "data_exportacao"]
                data_obj = datetime.strptime(data_str, "%Y-%m-%d")
                df.at[i, "data_exportacao"] = data_obj.strftime("%d/%m/%Y")
            except (ValueError, TypeError):
                pass
    
    # CORREÇÃO: Não convertemos para objetos datetime, mas para strings em diferentes formatos
    indices_to_convert = [i for i in range(10, len(df), 25) if i < len(df)]
    for i in indices_to_convert:
        try:
            data_str = df.at[i, "data_exportacao"]
            if isinstance(data_str, str):
                if "/" in data_str:
                    # Convertemos para outro formato, mas continuamos como string
                    data_obj = datetime.strptime(data_str, "%d/%m/%Y")
                    df.at[i, "data_exportacao"] = data_obj.strftime("%Y.%m.%d")
                else:
                    # Convertemos para outro formato, mas continuamos como string
                    data_obj = datetime.strptime(data_str, "%Y-%m-%d")
                    df.at[i, "data_exportacao"] = data_obj.strftime("%d-%m-%Y")
        except (ValueError, TypeError):
            pass
    
    # 2. Inserir valores nulos em modo_transporte e peso_total
    df.loc[df.sample(frac=0.03).index, "modo_transporte"] = None
    df.loc[df.sample(frac=0.02).index, "peso_total_kg"] = None
    
    # 3. "Sujar" códigos NCM
    for i in range(0, len(df), 30):
        if i < len(df):
            df.at[i, "ncm_exportado"] = df.at[i, "ncm_exportado"].replace(".", "")
    
    for i in range(5, len(df), 45):
        if i < len(df):
            df.at[i, "ncm_exportado"] = f" {df.at[i, 'ncm_exportado']} "
    
    # 4. Países com variações
    indices_50 = [i for i in range(0, len(df), 50) if i < len(df)]
    indices_75 = [i for i in range(0, len(df), 75) if i < len(df)]
    indices_90 = [i for i in range(0, len(df), 90) if i < len(df)]
    indices_100 = [i for i in range(0, len(df), 100) if i < len(df)]
    
    if indices_50:
        df.loc[indices_50, "destino"] = "México"
    if indices_75:
        df.loc[indices_75, "destino"] = "mexico"
    if indices_90:
        df.loc[indices_90, "destino"] = "EUA"
    if indices_100:
        df.loc[indices_100, "destino"] = "Estados unidos"
    
    # 5. Duplicar registros
    df = pd.concat([df, df.sample(frac=0.05)], ignore_index=True)
    
    # IMPORTANTE: Garantir que não há objetos datetime no DataFrame
    # Converter qualquer objeto datetime para string
    for col in df.columns:
        for i in range(len(df)):
            if isinstance(df.at[i, col], datetime):
                df.at[i, col] = df.at[i, col].strftime("%Y-%m-%d")
    
    output_path = "../data/raw/exportacoes_tratadas.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Dados de exportações gerados com sucesso: {output_path}")

def main():
    """Função principal para orquestrar a geração de todos os dados."""
    print("Iniciando geração de dados...")
    
    # 1. Gerar catálogo de peças
    id_pecas = gerar_catalogo_pecas()
    
    # 2. Gerar dados de estoque
    gerar_estoque(id_pecas)
    
    # 3. Gerar dados de exportações
    gerar_exportacoes(id_pecas)
    
    print("Geração de dados concluída com sucesso!")

if __name__ == "__main__":
    main()