import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_squared_error, accuracy_score, mean_absolute_error, confusion_matrix

class Visualizer(object):
    
    def __init_(self):
        pass
    
    def heatmap(self, df, title, figsize=(15, 8), annot=True, color=(10, 200)):
        correlation = df.corr()
        plt.figure(figsize=figsize)
        mask = np.zeros_like(correlation, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True

        cmap = sns.diverging_palette(color[0], color[1], as_cmap=True)
        plt.title(title)
        sns.heatmap(correlation, mask=mask, cmap=cmap, vmax=.5, center=0, 
                square=True, linewidths=.5,  annot=annot)
        
    def cf_matrix(self, y_true, y_pred, title, figsize=(15, 8), annot=True, color=(10, 200)):
        cfm = confusion_matrix(y_true, y_pred)
        plt.figure(figsize=figsize)
        cmap = sns.diverging_palette(color[0], color[1], as_cmap=True)
        plt.title(title)
        sns.heatmap(cfm, annot=annot, fmt=".2f", cbar=False, cmap=cmap)