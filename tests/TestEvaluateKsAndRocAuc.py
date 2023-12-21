import unittest
from libgal.modules.MLS import evaluate_ks_and_roc_auc


class TestEvaluateKsAndRocAuc(unittest.TestCase):

    def test_evaluate_ks_and_roc_auc(self):
        # Test case with known inputs and expected outputs
        y_real = [0, 0, 1, 1, 0, 1, 0, 1]
        y_proba = [0.2, 0.3, 0.6, 0.7, 0.4, 0.8, 0.1, 0.9]

        expected_ks_statistic = 1.0  # Replace with the expected KS statistic
        expected_roc_auc = 1.0  # Replace with the expected ROC AUC

        # Call the function
        ks_statistic, roc_auc, _ = evaluate_ks_and_roc_auc(y_real, y_proba)

        # Check if the actual output matches the expected output
        self.assertAlmostEqual(ks_statistic, expected_ks_statistic, places=4)
        self.assertAlmostEqual(roc_auc, expected_roc_auc, places=4)


if __name__ == '__main__':
    unittest.main()