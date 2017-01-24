import statsmodels.api as sm
import matplotlib.pyplot as plt

    dens = sm.nonparametric.KDEMultivariateConditional(endog=endog, exog=exog, dep_type='c', indep_type='c', bw=[bw, bw])  # endog, exog are input arrays, bw is tuned bandwith. In your case, you’ll want to iterate over possibilities for bandwidth and see what gives the best output

    endog = np.arange(-3.0, 3.0, 0.01)  # set up the boundaries of the pdf for plotting
    ys = dens.pdf(exog_predict=[sp]*len(endog), endog_predict=endog)  # sp is a given SPY move
    plt.plot(endog, ys, color='k', linewidth=1.0)
