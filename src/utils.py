import os
import sys
import Levenshtein



from src.exception import CustomException
import pyspark.sql.functions as F
import pyspark.sql.types as T

def detect_nulls(df, threshold, squema):

    try:
        df = df.select(squema)
        primer_fila = df.select([((F.sum(F.col(c).isNull().cast(T.IntegerType())) / df.count())).alias(c) for c in df.columns]).first()
        primer_fila_dict = {col: primer_fila[col] for col in df.columns if (primer_fila[col]<=threshold)}
        return list(primer_fila_dict.keys())
    

    except Exception as e:
        raise CustomException(e,sys)
    


def categoria_mas_parecida(cadena, lista_categorias):
    categoria_parecida = None
    distancia_minima = float('inf')
    
    for categoria in lista_categorias:
        distancia = Levenshtein.distance(cadena, categoria)
        
        if distancia < distancia_minima:
            distancia_minima = distancia
            categoria_parecida = categoria
            
    return categoria_parecida


        
if __name__=="__main__":

    list =  ['id_infraccion', 'fecha_infraccion', 'ao_infraccion', 'mes', 'categoria', 
                    'articulo', 'fraccion', 'inciso', 'parrafo', 'placa', 
                    'Color', 'marca_general', 'marca', 'submarca', 'en_la_calle', 
                    'entre_calle', 'y_calle', 'colonia', 'alcaldia', 'longitud', 'latitud']
    
    x = categoria_mas_parecida('color', list)

    print(x)