import unittest
import pandas as pd
from libgal.modules.MLS import NumNormTransformer


class TestNumNormTransformer(unittest.TestCase):

    def test_num_norm_transformer(self):
        # Test case with known inputs and expected outputs
        data = {
            'feature1_vl': [1, 2, 3, 4, 5],
            'feature2_vl': [5, 4, 3, 2, 1],
            'periodo_cd': [1, 2, 3, 4, 5],
            'cd_periodo': [5, 4, 3, 2, 1],
            'target': [0, 1, 0, 1, 0]
        }

        df = pd.DataFrame(data)

        transformer = NumNormTransformer(keep_original=True, sufix=['vl'], exclude=['periodo_cd', 'cd_periodo'])

        # Split the data into features (X) and target (y)
        X = df.drop('target', axis=1)
        y = df['target']

        # Fit and transform the data
        transformed_data = transformer.fit_transform(X, y)

        # Check if the transformed data has the expected columns
        expected_columns = list(data.keys())[:-1]
        assert len(list(transformed_data.columns)) == len(expected_columns), 'El tamaño de las columnas difiere'


if __name__ == '__main__':
    unittest.main()