import boto3
from pymongo import MongoClient

def obtener_datos():
    client = MongoClient('mongodb://user:password@mongo_service_name:27017/')
    db = client['gestion_usuario_db']
    coleccion = db['usuarios']
    datos = list(coleccion.find())
    return datos

def cargar_a_s3(data):
    s3 = boto3.client('s3')
    with open('/tmp/datos_usuarios.csv', 'w') as f:
        for doc in data:
            f.write(','.join(map(str, doc.values())) + '\n')
    s3.upload_file('/tmp/datos_usuarios.csv', 'tu_bucket_s3', 'datos_usuarios.csv')

if __name__ == "__main__":
    datos = obtener_datos()
    cargar_a_s3(datos)
