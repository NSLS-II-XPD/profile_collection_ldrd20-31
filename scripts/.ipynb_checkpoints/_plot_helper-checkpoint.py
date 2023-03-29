import matplotlib.pyplot as plt
import numpy
from side_functions import _readable_time
import _data_analysis as da


class open_figures():
    def __init__(self, figure_labels):
        for i in figure_labels:
            plt.figure(num=i)


class plot_uvvis(open_figures):
    
    def __init__(self, qepro_dic, metadata_dic, 
                 figure_labels = ['primary_absorbance', 'primary_fluorescence', 
                                  'bundle_absorbance', 'bundle_fluorescence'
                                  'peak fitting']):
        self.fig = figure_labels
        self.uid = metadata_dic['uid']
        self.stream_name = metadata_dic['stream_name']
        self.qepro_dic = qepro_dic
        self.metadata_dic = metadata_dic
        self.wavelength = []
        self.output = []   
        # self.num = None
        self.date, self.time = _readable_time(metadata_dic['time'])
        super().__init__(figure_labels)


    def plot_data(self):
        self.wavelength = self.qepro_dic['QEPro_x_axis']
        self.output = self.qepro_dic['QEPro_output']
        
        if self.stream_name == 'absorbance':
            y_label = 'Absorbance'
            try: f = plt.figure(self.fig[2])
            except (IndexError): f = plt.figure(self.fig[-1])
            ax = f.gca()
        
        elif self.stream_name == 'fluorescence':
            y_label = 'Fluorescence'
            try: f = plt.figure(self.fig[3])
            except (IndexError): f = plt.figure(self.fig[-1])
            ax = f.gca()
        
        elif self.stream_name == 'primary':
            if self.qepro_dic['QEPro_spectrum_type'] == 3:
                y_label = 'Absorbance'
            try: f = plt.figure(self.fig[0])
            except (IndexError): f = plt.figure(self.fig[-1])
                ax = f.gca()
            else:
                y_label = 'Fluorescence'
            try: f = plt.figure(self.fig[1])
            except (IndexError): f = plt.figure(self.fig[-1])
                ax = f.gca()

        for i in range(self.wavelength.shape[0]):
            ax.plot(self.wavelength[i], self.output[i], label=f'{self.time}_{i:03d}')
        # ax.set_facecolor((0.95, 0.95, 0.95))
        ax.set_xlabel('Wavelength (nm)', fontdict={'size': 14})
        ax.set_ylabel(y_label, fontdict={'size': 14})
        ax.set_title(f'{self.date}-{self.time}_{self.uid[0:8]}_{self.stream_name}')
        ax.legend()
        f.canvas.manager.show()
        f.canvas.flush_events()


    def plot_peak_fit(self, x, y, peak, fit_function, popt, fill_between=False):
        y_label = 'Fluorescence'
        
        try:
            f = plt.figure(self.fig[4])
        except (IndexError):
            f = plt.figure(self.fig[-1])
        
        ax = f.gca()
        
        fitted_y = fit_function(x, *popt)
        r_2 = da.r_square(x, y, fitted_y)
        r2 = f'R\u00b2={r_2:.2f}'
        ax.plot(x,y,'b+:',label='data')
        ax.plot(x,fitted_y,'r-:',label='Total fit\n'+r2)
        
        for i in range(len(peak)):
            ax.plot(x[peak[i]], y[peak[i]], '*', markersize=12)
        
        if fill_between:
            for i in range(len(peak)):
                pars_i = popt[0+3*i:3+3*i]
                peak_i = fit_function(x, *pars_i)
                ax.plot(x, peak_i, label=f'peak {i+1}')
                ax.fill_between(x, peak_i.min(), peak_i, alpha=0.3)

        # ax.set_facecolor((0.95, 0.95, 0.95))
        ax.set_xlabel('Wavelength (nm)', fontdict={'size': 14})
        ax.set_ylabel(y_label, fontdict={'size': 14})
        ax.set_title(f'{self.date}-{self.time}_{self.uid[0:8]}_{self.stream_name}_{fit_function.__name__}')
        ax.legend()
        f.canvas.manager.show()
        f.canvas.flush_events()
        
        