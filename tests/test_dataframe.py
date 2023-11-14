import pandas as pd
import random
from datetime import datetime, timedelta

nombres_animales = ['áspid', 'colibrí', 'tejón', 'mújol', 'tálamo', 'coendú', 'vicuña', 'ñandú', 'alacrán', 'armiño',
                    'pingüino', 'delfín', 'galápago', 'tiburón', 'murciélago', 'águila', 'ácana', 'tábano', 'caimán',
                    'tórtola', 'zángano', 'búfalo', 'dóberman', 'aúreo', 'cóndor', 'camaleón', 'nandú', 'órix',
                    'tucán', 'búho', 'pájaro']

nombres_personas = ['Valentina', 'Mateo', 'Laura', 'Camila', 'Renata', 'Sara', 'Amelia', 'Nicolás', 'Valery', 'Julio',
                    'Fernanda', 'Santiago', 'Isabella', 'Isabel', 'Joaquín', 'Emmanuel', 'Luciana', 'Ariana',
                    'Valeria', 'Dylan', 'Daniel', 'Juan', 'Lucas', 'Mariana', 'Sofía', 'Alejandro', 'Emma',
                    'Carlos', 'Ángel', 'Ana', 'Benjamín', 'Fabiola', 'Sebastián', 'Antonella', 'Gabriela', 'Diego',
                    'Esteban', 'Olivia', 'Emily', 'Adrián', 'Matías', 'Mía', 'Samuel', 'Leonardo', 'Emiliano',
                    'Gabriel', 'Victoria', 'Juliana']

apellidos_personas = ['Cruz', 'González', 'López', 'Herrera', 'Rivera', 'Molina', 'Rojas', 'Delgado', 'Ruiz',
                      'Sánchez', 'Castillo', 'Peralta', 'Guzmán', 'Pérez', 'Vargas', 'Vásquez', 'Castro',
                      'Silva', 'Romero', 'Gutiérrez', 'Ortiz', 'Mendoza', 'Álvarez', 'Ramírez', 'Ortega',
                      'Aguilar', 'Chávez', 'Núñez', 'Rodríguez', 'Padilla', 'Díaz', 'Gómez', 'Guerrero',
                      'Torres', 'García', 'Hernández', 'Morales', 'Reyes', 'Flores', 'Martínez', 'Campos',
                      'Jiménez', 'Estrada', 'Ramos']


def generate_dataframe(num_rows=1000000):

    # Columna 1: Fecha
    start_date = datetime(2024, 1, 1)
    date_column = [start_date + timedelta(seconds=i*300) for i in range(num_rows)]

    # Columna 2: Identificador único incremental
    id_column = list(range(1, num_rows + 1))

    # Columna 3: Nombre
    name_column = [random.choice(nombres_personas) for _ in range(num_rows)]

    # Columna 4: Apellido
    last_name_column = [random.choice(apellidos_personas) for _ in range(num_rows)]

    # Columna 5: Party_Id (Número entero)
    party_id_column = [random.randint(1000000, 10000000) for _ in range(num_rows)]

    # Columna 6: Valor de moneda random entre 0 y 10 millones
    currency_column = [random.uniform(0, 10000000) for _ in range(num_rows)]

    pet_column = [random.choice(nombres_animales) for _ in range(num_rows)]

    dict_df = {
        'Fecha_Dt': date_column,
        'Log_Id': id_column,
        'Nombre_Tx': name_column,
        'Apellido_Tx': last_name_column,
        'Party_Id': party_id_column,
        'Fondos_Amt': currency_column,
        'Animal_Favorito_Tx': pet_column
    }

    for i in range(0, 8):
        random_column = [random.uniform(0, 10000) for _ in range(num_rows)]
        dict_df[f'Columna_{i}_Amt'] = random_column

    for i in range(8, 22):
        random_column = [random.randint(10000, 100000) for _ in range(num_rows)]
        dict_df[f'Columna_{i}_Nu'] = random_column

    # Crear DataFrame
    df = pd.DataFrame(dict_df)

    return df
