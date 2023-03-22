import numpy as np
import matplotlib.pyplot as plt
from scipy import integrate  
#import scipy.signal as scipy
from scipy.optimize import curve_fit
from scipy.signal import find_peaks


## Fit a peak by 1 Gaussian or Lorentz distribution
## http://hyperphysics.phy-astr.gsu.edu/hbase/Math/gaufcn2.html
## https://en.wikipedia.org/wiki/Cauchy_distribution


def _1gauss(x, A, x0, sigma):
    return A * np.exp(-(x - x0) ** 2 / (2 * sigma ** 2))


def _1Lorentz(x, A, x0, sigma):
    return A*sigma**2/((x-x0)**2+sigma**2)


## Fit a peak by Multi Gaussian or Lorentz distributions
## http://hyperphysics.phy-astr.gsu.edu/hbase/Math/gaufcn2.html

def _2gauss(x, A1, x1, s1, A2, x2, s2):
    return (_1gauss(x, A1, x1, s1) +
            _1gauss(x, A2, x2, s2))


def _3gauss(x, A1, x1, s1, A2, x2, s2, A3, x3, s3):
    return (_1gauss(x, A1, x1, s1) +
            _1gauss(x, A2, x2, s2) + 
            _1gauss(x, A3, x3, s3))


def _2Lorentz(x, A1, x1, s1, A2, x2, s2):
    return (_1Lorentz(x, A1, x1, s1) +
            _1Lorentz(x, A2, x2, s2))


def _3Lorentz(x, A1, x1, s1, A2, x2, s2, A3, x3, s3):
    return (_1Lorentz(x, A1, x1, s1) +
            _1Lorentz(x, A2, x2, s2) +
            _1Lorentz(x, A3, x3, s3))



def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return idx, array[idx]



#### Criteria to identify a bad fluorescence peak ####
# c1. PL peak height < 2000 --> execute in scipy.find_peaks
# c2. PL peak wavelength < 560 nm, and the diference of peak integrstion (PL minus LED) < 100000
# c3. PL peak wavelength >= 560 nm, and the diference of peak integrstion (PL minus LED) < 200000
def good_bad_data(x, y, key_height = 2000, data_id = 'test', distance=30, height=30, 
                  c2_c3 = False, threshold=[560, 100000, 200000], int_boundary = [340, 400, 800] 
                 ):
    
    peak, prop = find_peaks(y, height=height, distance=distance)

    if (len(int_boundary)>=3 and c2_c3==True):
        w1, _ = find_nearest(x, int_boundary[0])
        w2, _ = find_nearest(x, int_boundary[1])
        w3, _ = find_nearest(x, int_boundary[2])

        LED_integration = integrate.simpson(y[w1:w2])
        PL_integration = integrate.simpson(y[w2:w3])
        peak_diff = PL_integration - LED_integration

    peak_heights_2 = []
    for i in peak:
        if x[i] < 400:
            peak_heights_2.append(0.0)
        else:
            peak_heights_2.append(y[i])

    c1 = (max(peak_heights_2) < key_height)

    c2, c3 = False, False
    if c2_c3 == True:
        try:
            c2 = (x[peak[-1]]<threshold[0] and peak_diff < threshold[1])
        except IndexError, TypeError:
            pass
        
        try:
            c3 = (x[peak[-1]]<threshold[0] and peak_diff < threshold[2])
        except IndexError, TypeError:
            pass
       
    if c1:
        print(f'Spectra {data_id} is bad.')
        return False
    elif c2:
        print(f'Spectra {data_id} is bad.')
        return False
    elif c3:
        print(f'Spectra {data_id} is bad.')
        return False
    else:
        print(f'Spectra {data_id} is good.')
        return True


def _2peak_fit_PL3(x, y, distr='G', distance=30, height=930, plot=False, plot_title=None, second_peak=None, maxfev=100000):
    # 'G': Guassian
    # 'L': Lorentz  
    peak, _ = find_peaks(y, distance=distance, height=height)
    # peaks=[peak[0]]
    # for i in range(1, len(peak)):
    #     if peak[i]-peak[i-1]>20:
    #         peaks.append(peak[i])
        
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))
    
    if len(peaks) == 2:
        if distr == 'G':
            popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], maxfev=maxfev)
    else:
        while len(peak) > 2:
        try:
            len(peak) == 2
    except:
        len(peak)   

        if abs(second_peak)<20:
            if distr == 'G' and abs(second_peak)<20:
                popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
            else:
                popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak*sigma), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
            
        else:
            if distr == 'G' and abs(second_peak)>=20:
                popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[find_nearest(x, second_peak)[0]], second_peak, sigma], maxfev=maxfev)
            else:
                popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak*sigma), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
    #A = popt[0]
    #x0 = popt[1]
    #sigma = popt[2]
    
    pars_1 = popt[0:3]
    pars_2 = popt[3:6]
    
    if distr == 'G':
        peak_1 = _1gauss(x, *pars_1)
        peak_2 = _1gauss(x, *pars_2)
        fit_model = 'Gaussian'
    else:
        peak_1 = _1Lorentz(x, *pars_1)
        peak_2 = _1Lorentz(x, *pars_2)
        fit_model = 'Lorentz'
    
    fitted_result = _2gauss(x, *popt)
    residulas = y - fitted_result
    ss_res = np.sum(residulas**2)
    ss_tot = np.sum((y-np.mean(y))**2)
    r_2 = 1 - (ss_res / ss_tot)
    r2 = f'R\u00b2={r_2:.2f}'
    
    if plot == True:
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        
        # peak 1
        plt.plot(x, peak_1, "g", label='peak 1')
        plt.fill_between(x, peak_1.min(), peak_1, facecolor="green", alpha=0.5)
  
        # peak 2
        plt.plot(x, peak_2, "y", label='peak 2')
        plt.fill_between(x, peak_2.min(), peak_2, facecolor="yellow", alpha=0.5)  
        
        plt.title(f'{fit_model} : {plot_title}')
        plt.legend()
        plt.show()
    else: pass
    
    return popt, r_2



def _3peak_fit_PL2(x, y, distr='G', height=930, plot=False, plot_title=None, second_peak=None, third_peak=None,maxfev=100000):
    # 'G': Guassian
    # 'L': Lorentz  
    peak, _ = find_peaks(y, height=height)
    peaks=[peak[0]]
    for i in range(1, len(peak)):
        if peak[i]-peak[i-1]>20:
            peaks.append(peak[i])
    
    if len(peaks) >3:
        raise IndexError('Number of peaks should be less than 2.')
        
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))

    
    if len(peaks) == 3:
        if distr == 'G':
            popt, pcov = curve_fit(_3gauss, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[1]], x[peaks[1]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], 
                                   maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_3Lorentz, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[1]], x[peaks[1]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], 
                                   maxfev=maxfev)
    else:            
        if distr == 'G':
            popt, pcov = curve_fit(_3gauss, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[find_nearest(x, second_peak)[0]], second_peak, sigma, y[find_nearest(x, third_peak)[0]], third_peak, sigma], 
                                   maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_3Lorentz, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[find_nearest(x, second_peak)[0]], second_peak, sigma, y[find_nearest(x, third_peak)[0]], third_peak, sigma], 
                                   maxfev=maxfev)
    #A = popt[0]
    #x0 = popt[1]
    #sigma = popt[2]
    
    pars_1 = popt[0:3]
    pars_2 = popt[3:6]
    pars_3 = popt[6:9]
    
    if distr == 'G':
        peak_1 = _1gauss(x, *pars_1)
        peak_2 = _1gauss(x, *pars_2)
        peak_3 = _1gauss(x, *pars_3)
        fit_model = 'Gaussian'
    else:
        peak_1 = _1Lorentz(x, *pars_1)
        peak_2 = _1Lorentz(x, *pars_2)
        peak_3 = _1gauss(x, *pars_3)
        fit_model = 'Lorentz'
    
    fitted_result = _3gauss(x, *popt)
    residulas = y - fitted_result
    ss_res = np.sum(residulas**2)
    ss_tot = np.sum((y-np.mean(y))**2)
    r_2 = 1 - (ss_res / ss_tot)
    r2 = f'R\u00b2={r_2:.2f}'
    
    if plot == True:
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        
        # peak 1
        plt.plot(x, peak_1, "g", label='peak 1')
        plt.fill_between(x, peak_1.min(), peak_1, facecolor="green", alpha=0.5)
  
        # peak 2
        plt.plot(x, peak_2, "y", label='peak 2')
        plt.fill_between(x, peak_2.min(), peak_2, facecolor="yellow", alpha=0.5)
        
        # peak 3
        plt.plot(x, peak_3, "b", label='peak 3')
        plt.fill_between(x, peak_3.min(), peak_3, facecolor="blue", alpha=0.5)
        
        plt.title(f'{fit_model} : {plot_title}')
        plt.legend()
        plt.show()
    else: pass
    
    return popt, r_2




######## Old versions of function #########

def _2peak_fit_PL2(x, y, distr='G', height=930, plot=False, plot_title=None, second_peak=None, maxfev=100000):
    # 'G': Guassian
    # 'L': Lorentz  
    peak, _ = find_peaks(y, height=height)
    peaks=[peak[0]]
    for i in range(1, len(peak)):
        if peak[i]-peak[i-1]>20:
            peaks.append(peak[i])
    
    if len(peaks) >2:
        raise IndexError('Number of peaks should be less than 2.')
        
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))
    
    if len(peaks) == 2:
        if distr == 'G':
            popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], maxfev=maxfev)
    else:
        if abs(second_peak)<20:
            if distr == 'G' and abs(second_peak)<20:
                popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
            else:
                popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak*sigma), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
            
        else:
            if distr == 'G' and abs(second_peak)>=20:
                popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[find_nearest(x, second_peak)[0]], second_peak, sigma], maxfev=maxfev)
            else:
                popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak*sigma), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
    #A = popt[0]
    #x0 = popt[1]
    #sigma = popt[2]
    
    pars_1 = popt[0:3]
    pars_2 = popt[3:6]
    
    if distr == 'G':
        peak_1 = _1gauss(x, *pars_1)
        peak_2 = _1gauss(x, *pars_2)
        fit_model = 'Gaussian'
    else:
        peak_1 = _1Lorentz(x, *pars_1)
        peak_2 = _1Lorentz(x, *pars_2)
        fit_model = 'Lorentz'
    
    fitted_result = _2gauss(x, *popt)
    residulas = y - fitted_result
    ss_res = np.sum(residulas**2)
    ss_tot = np.sum((y-np.mean(y))**2)
    r_2 = 1 - (ss_res / ss_tot)
    r2 = f'R\u00b2={r_2:.2f}'
    
    if plot == True:
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        
        # peak 1
        plt.plot(x, peak_1, "g", label='peak 1')
        plt.fill_between(x, peak_1.min(), peak_1, facecolor="green", alpha=0.5)
  
        # peak 2
        plt.plot(x, peak_2, "y", label='peak 2')
        plt.fill_between(x, peak_2.min(), peak_2, facecolor="yellow", alpha=0.5)  
        
        plt.title(f'{fit_model} : {plot_title}')
        plt.legend()
        plt.show()
    else: pass
    
    return popt, r_2



def _3peak_fit_PL2(x, y, distr='G', height=930, plot=False, plot_title=None, second_peak=None, third_peak=None,maxfev=100000):
    # 'G': Guassian
    # 'L': Lorentz  
    peak, _ = find_peaks(y, height=height)
    peaks=[peak[0]]
    for i in range(1, len(peak)):
        if peak[i]-peak[i-1]>20:
            peaks.append(peak[i])
    
    if len(peaks) >3:
        raise IndexError('Number of peaks should be less than 2.')
        
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))

    
    if len(peaks) == 3:
        if distr == 'G':
            popt, pcov = curve_fit(_3gauss, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[1]], x[peaks[1]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], 
                                   maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_3Lorentz, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[1]], x[peaks[1]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], 
                                   maxfev=maxfev)
    else:            
        if distr == 'G':
            popt, pcov = curve_fit(_3gauss, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[find_nearest(x, second_peak)[0]], second_peak, sigma, y[find_nearest(x, third_peak)[0]], third_peak, sigma], 
                                   maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_3Lorentz, x, y, 
                                   p0=[y[peaks[0]], x[peaks[0]], sigma, y[find_nearest(x, second_peak)[0]], second_peak, sigma, y[find_nearest(x, third_peak)[0]], third_peak, sigma], 
                                   maxfev=maxfev)
    #A = popt[0]
    #x0 = popt[1]
    #sigma = popt[2]
    
    pars_1 = popt[0:3]
    pars_2 = popt[3:6]
    pars_3 = popt[6:9]
    
    if distr == 'G':
        peak_1 = _1gauss(x, *pars_1)
        peak_2 = _1gauss(x, *pars_2)
        peak_3 = _1gauss(x, *pars_3)
        fit_model = 'Gaussian'
    else:
        peak_1 = _1Lorentz(x, *pars_1)
        peak_2 = _1Lorentz(x, *pars_2)
        peak_3 = _1gauss(x, *pars_3)
        fit_model = 'Lorentz'
    
    fitted_result = _3gauss(x, *popt)
    residulas = y - fitted_result
    ss_res = np.sum(residulas**2)
    ss_tot = np.sum((y-np.mean(y))**2)
    r_2 = 1 - (ss_res / ss_tot)
    r2 = f'R\u00b2={r_2:.2f}'
    
    if plot == True:
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        
        # peak 1
        plt.plot(x, peak_1, "g", label='peak 1')
        plt.fill_between(x, peak_1.min(), peak_1, facecolor="green", alpha=0.5)
  
        # peak 2
        plt.plot(x, peak_2, "y", label='peak 2')
        plt.fill_between(x, peak_2.min(), peak_2, facecolor="yellow", alpha=0.5)
        
        # peak 3
        plt.plot(x, peak_3, "b", label='peak 3')
        plt.fill_between(x, peak_3.min(), peak_3, facecolor="blue", alpha=0.5)
        
        plt.title(f'{fit_model} : {plot_title}')
        plt.legend()
        plt.show()
    else: pass
    
    return popt, r_2



def _1peak_fit_PL(x, y, distr='G', plot=False, plot_title=None, maxfev=100000):    
    # 'G': Guassian
    # 'L': Lorentz        
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))
    if distr == 'G':
        popt, pcov = curve_fit(_1gauss, x, y, p0=[max(y), mean, sigma], maxfev=maxfev)
    else:
        popt, pcov = curve_fit(_1Lorentz, x, y, p0=[max(y), mean, sigma], maxfev=maxfev)
    
    A = popt[0]
    x0 = popt[1]
    sigma = popt[2]
    
    if distr == 'G':
        fitted_result = _1gauss(x, *popt)
        fit_model = 'Gaussian'
    else:
        fitted_result = _1Lorentz(x, *popt)
        fit_model = 'Lorentz'
    
    #fitted_result = _1gauss(x, *popt)
    residulas = y - fitted_result
    ss_res = np.sum(residulas**2)
    ss_tot = np.sum((y-np.mean(y))**2)
    r_2 = 1 - (ss_res / ss_tot)
    r2 = f'R\u00b2={r_2:.2f}'
    
    if plot == True:
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        plt.legend()
        plt.title(f'{fit_model} : {plot_title}')
        plt.show()
    else: pass
    
    return popt, r_2


def _2peak_fit_PL(x, y, distr='G', height=930, plot=False, plot_title=None, second_peak=3, maxfev=100000):
    # 'G': Guassian
    # 'L': Lorentz  
    peaks, _ = find_peaks(y, height=height)
    if len(peaks) >2:
        raise IndexError('Number of peaks should be less than 2.')
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))
    
    if len(peaks) == 2:
        if distr == 'G':
            popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[-1]], x[peaks[-1]], sigma], maxfev=maxfev)
    else:
        if distr == 'G':
            popt, pcov = curve_fit(_2gauss, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
        else:
            popt, pcov = curve_fit(_2Lorentz, x, y, p0=[y[peaks[0]], x[peaks[0]], sigma, y[peaks[0]]/abs(second_peak*sigma), x[peaks[0]]-second_peak*sigma, sigma], maxfev=maxfev)
    #A = popt[0]
    #x0 = popt[1]
    #sigma = popt[2]
    
    pars_1 = popt[0:3]
    pars_2 = popt[3:6]
    
    if distr == 'G':
        peak_1 = _1gauss(x, *pars_1)
        peak_2 = _1gauss(x, *pars_2)
        fit_model = 'Gaussian'
    else:
        peak_1 = _1Lorentz(x, *pars_1)
        peak_2 = _1Lorentz(x, *pars_2)
        fit_model = 'Lorentz'
    
    fitted_result = _2gauss(x, *popt)
    residulas = y - fitted_result
    ss_res = np.sum(residulas**2)
    ss_tot = np.sum((y-np.mean(y))**2)
    r_2 = 1 - (ss_res / ss_tot)
    r2 = f'R\u00b2={r_2:.2f}'
    
    if plot == True:
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        
        # peak 1
        plt.plot(x, peak_1, "g", label='peak 1')
        plt.fill_between(x, peak_1.min(), peak_1, facecolor="green", alpha=0.5)
  
        # peak 2
        plt.plot(x, peak_2, "y", label='peak 2')
        plt.fill_between(x, peak_2.min(), peak_2, facecolor="yellow", alpha=0.5)  
        
        plt.title(f'{fit_model} : {plot_title}')
        plt.legend()
        plt.show()
    else: pass
    
    return popt, r_2
