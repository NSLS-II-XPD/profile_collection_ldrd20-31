import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy import integrate  
#import scipy.signal as scipy
from scipy.optimize import curve_fit
from scipy.signal import find_peaks
import _data_export as de



## Read meta data (fitting result) in csv
def _read_meta_csv(fn, header=14):
    meta = {}
    with open (fn, 'r') as f:
        temp = f.readlines()[:header]
        for i in range(len(temp)):
            t = temp[i].strip('\n').split(',')
            meta[t[0]] = t[1:]
    return meta



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



def r_square(x, y, fitted_y):
    residulas = y - fitted_y
    ss_res = np.sum(residulas**2)
    ss_tot = np.sum((y-np.mean(y))**2)
    r_sq = 1 - (ss_res / ss_tot)
    return r_sq


#### Criteria to identify a bad fluorescence peak ####
# c1. PL peak height < 2000 --> execute in scipy.find_peaks
# c2. PL peak wavelength < 560 nm, and the diference of peak integrstion (PL minus LED) < 100000
# c3. PL peak wavelength >= 560 nm, and the diference of peak integrstion (PL minus LED) < 200000
def good_bad_data(x, y, key_height = 2000, data_id = 'test', distance=30, height=30, 
                  c2_c3 = False, threshold=[560, 100000, 200000], int_boundary = [340, 400, 800], 
                  dummy_test = False):
    
    # Identify all peaks, including LED source, of UVVIS spectrum
    peak, prop = find_peaks(y, height=height, distance=distance)

    # Divide the spectrum into 2 regions for integration:
    # 1. 340 nm - 400 nm : LED source
    # 2. 400 nm - 800 nm : fluorescence
    if (len(int_boundary)>=3 and c2_c3==True):
        w1, _ = find_nearest(x, int_boundary[0])
        w2, _ = find_nearest(x, int_boundary[1])
        w3, _ = find_nearest(x, int_boundary[2])

        LED_integration = integrate.simpson(y[w1:w2])
        PL_integration = integrate.simpson(y[w2:w3])
        peak_diff = PL_integration - LED_integration

    peak_heights_2 = []
    peak2 = []
    prop2 = {'peak_heights':[]}

    # Take LED peak intensity off and only consider peaks > 400 nm for good/bad data because
    # 1. sometines LED peak has very high intensity which is larger than key_height
    # 2. fitting function depends on the # of peaks excluding the LED peak

    for i in peak:
        if dummy_test:
            peak_heights_2.append(y[i])
            peak2.append(i)
            prop2['peak_heights'].append(y[i])
        else:
            if x[i] < 400:
                peak_heights_2.append(0.0)
            else:
                peak_heights_2.append(y[i])
                peak2.append(i)
                prop2['peak_heights'].append(y[i])

    max_idx = peak_heights_2.index(max(peak_heights_2))

    # c1 equls True which indicates bad data
    c1 = (peak_heights_2[max_idx] < key_height)

    c2, c3 = False, False
    if c2_c3 == True:
        try:
            # c2 equls True and c3 equls None which indicates bad data
            if x[peak[max_idx]]<threshold[0]:
                c3 = None
                c2 = (x[peak[max_idx]]<threshold[0] and peak_diff < threshold[1])
            
            # c2 equls None and c3 equls Turn which indicates bad data
            else:
                c2 = None
                c3 = (x[peak[max_idx]]>threshold[0] and peak_diff < threshold[2])
        except (IndexError, TypeError):
            pass
       
    # when c1, c2, or c3 is True which indicates bad data, return peak2 and prop2 as []
    if c1:
        print(f'{data_id} is bad due to a low peak height (c1).')
        peak2, prop2 = [], []
        return peak2, prop2
    elif c2:
        print(f'{data_id} is bad due to a low peak integartion (c2).')
        peak2, prop2 = [], []
        return peak2, prop2
    elif c3:
        print(f'{data_id} is bad due to a low peak integartion (c3).')
        peak2, prop2 = [], []
        return peak2, prop2
    
    
    # Otherwise return peak2 and prop2 for good data
    else:
        if c2_c3 == False:
            print(f'{data_id} passes c1 so is good.')
        else:
            if c3 == None:
                print(f'{data_id} passes c1, c2, so is good.')
            if c2 == None:
                print(f'{data_id} passes c1, c3 so is good.')
    
        # therefore if data is good, peak2 will be an array and prop2 will be {}.
        return np.asarray(peak2), prop2





def _1peak_fit_good_PL(x0, y0, fit_function, peak=False, maxfev=100000, fit_boundary=[340, 400, 800], raw_data=False,
                       plot=False, plot_title=None, dummy_test=False):    
    try:
        w1, _ = find_nearest(x0, fit_boundary[0])
        w2, _ = find_nearest(x0, fit_boundary[1])
        w3, _ = find_nearest(x0, fit_boundary[2])
    except IndexError:
        w1, _ = find_nearest(x0, 340)
        w2, _ = find_nearest(x0, 400)
        w3, _ = find_nearest(x0, 800)
    
    if dummy_test:
        x = x0[w1:w2]
        y = y0[w1:w2]
    else:         
        x = x0[w2:w3]
        y = y0[w2:w3]
    
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))
    
    
    try:
        initial_guess = [y0[peak[-1]], x0[peak[-1]], sigma]
    except (TypeError, IndexError):
        initial_guess = [max(y), mean, sigma]
    
    
    try:
        popt, pcov = curve_fit(fit_function, x, y, p0=initial_guess, maxfev=maxfev)
    except RuntimeError:
        maxfev=1000000
        popt, pcov = curve_fit(fit_function, x, y, p0=initial_guess, maxfev=maxfev)
    
    # A = popt[0]
    # x0 = popt[1]
    # sigma = popt[2]

    
    if plot == True:
        fitted_result = fit_function(x, *popt)
        r_2 = r_square(x, y, fitted_result)
        r2 = f'R\u00b2={r_2:.2f}'
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        plt.legend()
        plt.title(f'{fit_function.__name__} : {plot_title}')
        plt.show()
    else: pass
    
    if raw_data:
        return popt, pcov, x, y
    else:
        return popt, pcov
    



def _2peak_fit_good_PL(x0, y0, fit_function, peak=False, maxfev=100000, fit_boundary=[340, 400, 800], raw_data=False, 
                       second_peak=None, plot=False, plot_title=None):    
    try:
        w1, _ = find_nearest(x0, fit_boundary[0])
        w2, _ = find_nearest(x0, fit_boundary[1])
        w3, _ = find_nearest(x0, fit_boundary[2])
    except IndexError:
        w1, _ = find_nearest(x0, 340)
        w2, _ = find_nearest(x0, 400)
        w3, _ = find_nearest(x0, 800)
    
    
    x = x0[w2:w3]
    y = y0[w2:w3]
    mean = sum(x * y) / sum(y)
    sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))
    
    
    try:
        initial_guess = [y.max(), x[y.argmax()], sigma, y[find_nearest(x, second_peak)[0]], second_peak, sigma]
    except (TypeError, IndexError):
        initial_guess = [y0[peak[0]], x0[peak[0]], sigma, y0[peak[-1]], x0[peak[-1]], sigma]
    
    
    try:
        popt, pcov = curve_fit(fit_function, x, y, p0=initial_guess, maxfev=maxfev)
    except RuntimeError:
        maxfev=1000000
        popt, pcov = curve_fit(fit_function, x, y, p0=initial_guess, maxfev=maxfev)
    
    # A = popt[0]
    # x0 = popt[1]
    # sigma = popt[2]
   
    if plot == True:
        fitted_result = fit_function(x, *popt)
        r_2 = r_square(x, y, fitted_result)
        r2 = f'R\u00b2={r_2:.2f}'
        plt.figure()
        plt.plot(x,y,'b+:',label='data')
        plt.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)
        
        pars_1 = popt[0:3]
        pars_2 = popt[3:6]
        peak_1 = fit_function(x, *pars_1)
        peak_2 = fit_function(x, *pars_2)
        
        # peak 1
        plt.plot(x, peak_1, "g", label='peak 1')
        plt.fill_between(x, peak_1.min(), peak_1, facecolor="green", alpha=0.5)
  
        # peak 2
        plt.plot(x, peak_2, "y", label='peak 2')
        plt.fill_between(x, peak_2.min(), peak_2, facecolor="yellow", alpha=0.5)  
        
        plt.legend()
        plt.title(f'{fit_function.__name__} : {plot_title}')
        plt.show()
    else: pass
    
    if raw_data:
        return popt, pcov, x, y
    else:
        return popt, pcov




def _identify_one_in_kafka(qepro_dic, metadata_dic, key_height=200, distance=100, height=50, dummy_test=False):
    _, time1 = de._readable_time(metadata_dic['time'])
    data_id = time1 + '_' + metadata_dic['uid'][:8]
    x0 = qepro_dic['QEPro_x_axis'][0]
    y0 = qepro_dic['QEPro_output'][0]
    peak, prop = good_bad_data(x0, y0, key_height=key_height, data_id = f'{data_id}', distance=distance, height=height, dummy_test=dummy_test)
    return x0, y0, data_id, peak, prop




def _identify_multi_in_kafka(qepro_dic, metadata_dic, key_height=200, distance=100, height=50, dummy_test=False):
    _, time1 = de._readable_time(metadata_dic['time'])
    data_id = time1 + '_' + metadata_dic['uid'][:8]
    _for_average = pd.DataFrame()
    for i in range(qepro_dic['QEPro_spectrum_type'].shape[0]):
        x_i = qepro_dic['QEPro_x_axis'][i]
        y_i = qepro_dic['QEPro_output'][i]
        p1, p2 = good_bad_data(x_i, y_i, key_height=key_height, data_id = f'{data_id}_{i:03d}', distance=distance, height=height, dummy_test=dummy_test)
        if (type(p1) is np.ndarray) and (type(p2) is dict):
            _for_average[f'{data_id}_{i:03d}'] = y_i
    
    _for_average[f'{data_id}_mean'] = _for_average.mean(axis=1)
    
    x0 = x_i
    y0 = _for_average[f'{data_id}_mean'].values
    
    peak, prop = good_bad_data(x0, y0, key_height=key_height, data_id = f'{data_id}_average', distance=distance, height=height, dummy_test=dummy_test)                            
    return x0, y0, data_id, peak, prop

    
    
def _fitting_in_kafka(x0, y0, data_id, peak, prop, dummy_test=False):
    print(f'\n** Average of {data_id} has peaks at {peak}**\n')
    
    print(f'\n** start to do peak fitting by Gaussian**\n')
    if len(peak) == 1:
        f = _1gauss
        popt, _, x, y = _1peak_fit_good_PL(x0, y0, f, peak=peak, raw_data=True, dummy_test=dummy_test)
    elif len(peak) == 2:
        f = _2gauss
        popt, _, x, y = _2peak_fit_good_PL(x0, y0, f, peak=peak, raw_data=True)
    else:
        f = _1gauss
        M = max(prop['peak_heights'])
        M_idx, _ = find_nearest(prop['peak_heights'], M)
        peak = np.asarray([peak[M_idx]])
        popt, _, x, y = _1peak_fit_good_PL(x0, y0, f, peak=peak, raw_data=True, dummy_test=dummy_test)

    shift, _ = find_nearest(x0, x[0])

    return x, y, peak-shift, f, popt

    
    
######## Old versions of function #########    

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



