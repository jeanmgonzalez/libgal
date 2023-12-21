from collections import defaultdict
import numpy as np
from scipy.stats import ks_2samp
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.metrics import roc_curve, roc_auc_score
import pandas


# FUNCIONES DE MACHINE LEARNING

def evaluate_ks_and_roc_auc(y_real, y_proba):
    # Unite both visions to be able to filter
    df = pandas.DataFrame()
    df['real'] = y_real
    df['proba'] = y_proba
    class0 = df[df['real'] == 0]
    class1 = df[df['real'] == 1]
    ks = ks_2samp(class0['proba'], class1['proba'])
    roc_auc = roc_auc_score(df['real'], df['proba'])

    print(f"KS: {ks.statistic:.4f} (p-value: {ks.pvalue:.3e})")
    print(f"ROC AUC: " + str(roc_auc))
    return ks.statistic, roc_auc, df


class NumNormTransformer(BaseEstimator, TransformerMixin):
    # the constructor
    def __init__(self, keep_original=True, sufix=['vl'], exclude=['periodo_cd', 'cd_periodo']):
        self.keep_original = keep_original
        self.sufix = sufix
        self.exclude = exclude

    # estimator method
    def fit(self, X, y=None):
        return self

    # transfprmation
    def transform(self, X, y=None):
        self.originals = []
        for col in X.columns:
            for suf in self.sufix:
                if (col.endswith(suf) or col.startswith(suf)) and col not in self.exclude:
                    self.originals.append(col)

        self.X_norm = X[self.originals]
        self.X_norm.columns = [str(x) + "_norm" for x in self.X_norm]
        self.X_norm = (self.X_norm - self.X_norm.mean()) / (self.X_norm.std())

        self.X_sca = X[self.originals]
        self.X_sca.columns = [str(x) + "_sca" for x in self.X_sca]
        self.X_sca = (self.X_sca.max() - self.X_sca) / (self.X_sca.max() - self.X_sca.min())
        X.drop(columns=self.originals, inplace=True)

        X = pandas.concat([X, self.X_norm], axis=1)
        return X


class NumLogTransformer(BaseEstimator, TransformerMixin):
    # the constructor
    def __init__(self, keep_original=True, sufix=['vl'], exclude=['periodo_cd', 'cd_periodo']):
        self.keep_original = keep_original
        self.sufix = sufix
        self.exclude = exclude

    # estimator method
    def fit(self, X, y=None):
        return self

    # transfprmation
    def transform(self, X, y=None):
        self.originals = []
        for col in X.columns:
            for suf in self.sufix:
                if (col.endswith(suf) or col.startswith(suf)) and col not in self.exclude:
                    self.originals.append(col)

        self.X_log = X[self.originals]
        self.X_log.columns = [str(x) + "_log" for x in self.X_log]
        self.X_log = np.log(self.X_log + 1)
        X = pandas.concat([X, self.X_log], axis=1)
        if not self.keep_original:
            X.drop(columns=self.originals, inplace=True)
        return X


class CategoricalReduceTransformer(BaseEstimator, TransformerMixin):
    # the constructor
    def __init__(self, keep_original=False, sufix=['tx', 'cd'], threshold=0.99, exclude=['periodo_cd', 'cd_periodo']):
        self.keep_original = keep_original
        self.sufix = sufix
        self.threshold = threshold
        self.exclude = exclude

    # estimator method
    def fit(self, X, y=None):
        return self

    # transfprmation
    def transform(self, X, y=None):
        self.originals = []
        for col in X.columns:
            for suf in self.sufix:
                if (col.endswith(suf) or col.startswith(suf)) and col not in self.exclude:
                    self.originals.append(col)
                    D = X.groupby([col], as_index=False).size().rename(columns={'size': 'n'}).sort_values('n',
                                                                                                          ascending=False)
                    D['p'] = D['n'] / D['n'].sum()
                    D['cumsum_p'] = D['p'].cumsum()
                    dict_estados = defaultdict(lambda: 'Otros')
                    for i in range(len(D)):
                        if D.cumsum_p[i] < self.threshold:
                            dict_estados[D[col][i]] = str(D[col][i]).replace(".", "").replace("°", "").replace(">",
                                                                                                               " ").replace(
                                "<", " ").replace("/", " ")
                        else:
                            dict_estados[D[col][i]] = "Otros"
                    if self.keep_original:
                        X["freq_" + str(col)] = [dict_estados[x] for x in X[col]]
                    else:
                        X[str(col)] = [dict_estados[x] for x in X[col]]

        return X
