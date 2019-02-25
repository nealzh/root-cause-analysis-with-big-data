import warnings
warnings.filterwarnings("ignore")
import numpy as np
import statsmodels.api as sm


class smoothing_method:
    """
    This class is to simulate Yield Cube smoothing method

    """
    def __init__(self, x, y):
        """

        :param x: array of x
        :param y: array of y
        """
        self.x = x
        self.y = y

    def sigma_selection(self):
        """

        :return: sigma number
        """
        sigma_selection = float(min((float(np.percentile(self.x, 75)) - float(np.percentile(self.x, 25))) / 1.34,
                                    float(np.std(self.x))))
        return sigma_selection

    def bandwidth_estimation(self):
        """

        :return: bandwidth used for kernel smoothing
        """
        sigma = float(self.sigma_selection())
        n = self.y.shape[0]
        bandwidth = float(1.06 * float(sigma) * (float(n) ** (-0.2)))
        return bandwidth

    def smoothing_pred(self):
        """

        :param bandwidth:
        :return: the prediction value based on kernel smoothing
        """
        bandwidth = self.bandwidth_estimation()
        y_cap = sm.nonparametric.KernelReg(self.y, self.x, 'c', 'lc', bw=np.asarray([bandwidth], dtype=np.float64))
        return y_cap
