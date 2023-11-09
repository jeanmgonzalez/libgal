import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

nombres_animales = ['áspid', 'colibrí', 'tejón', 'mújol', 'tálamo', 'coendú', 'vicuña', 'ñandú', 'alacrán', 'armiño',
                    'pingüino', 'delfín', 'galápago', 'tiburón', 'murciélago', 'águila', 'ácana', 'tábano', 'caimán',
                    'tórtola', 'zángano', 'búfalo', 'dóberman', 'aúreo', 'cóndor', 'camaleón', 'nandú', 'órix',
                    'tucán', 'búho', 'pájaro']


def generate_dataframe(num_rows=1000000):
    fake = Faker()

    # Configuración de Faker para generar datos realistas
    Faker.seed(0)

    # Columna 1: Fecha
    start_date = datetime(2024, 1, 1)
    date_column = [start_date + timedelta(seconds=i) for i in range(num_rows)]

    # Columna 2: Identificador único incremental
    id_column = list(range(1, num_rows + 1))

    # Columna 3: Nombre
    name_column = [fake.first_name() for _ in range(num_rows)]

    # Columna 4: Apellido
    last_name_column = [fake.last_name() for _ in range(num_rows)]

    # Columna 5: Party_Id (Número entero)
    party_id_column = [random.randint(1000000, 10000000) for _ in range(num_rows)]

    # Columna 6: Valor de moneda random entre 0 y 10 millones
    currency_column = [random.uniform(0, 10000000) for _ in range(num_rows)]

    pet_column = [random.choice(nombres_animales) for _ in range(num_rows)]

    dict_df = {
        'Fecha': date_column,
        'ID': id_column,
        'Nombre': name_column,
        'Apellido': last_name_column,
        'Party_Id': party_id_column,
        'Valor_Moneda': currency_column,
        'Animal_Favorito': pet_column
    }

    for i in range(0, 8):
        random_column = [random.uniform(0, 10000) for _ in range(num_rows)]
        dict_df[f'Columna_{i}'] = random_column

    for i in range(8, 22):
        random_column = [random.randint(10000, 100000) for _ in range(num_rows)]
        dict_df[f'Columna_{i}'] = random_column

    # Crear DataFrame
    df = pd.DataFrame(dict_df)

    return df
