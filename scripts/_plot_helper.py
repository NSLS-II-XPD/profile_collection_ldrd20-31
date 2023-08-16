import matplotlib.pyplot as plt
import numpy
from _data_export import _readable_time
import _data_analysis as da


class open_figures():
    def __init__(self, figure_labels):
        for i in figure_labels:
            plt.figure(num=i)


class plot_uvvis(open_figures):
    
    def __init__(self, qepro_dic, metadata_dic, 
                 figure_labels = ['primary_absorbance', 'primary_fluorescence', 
                                  'bundle_absorbance', 'bundle_fluorescence',
                                  'peak fitting', 'Spectra Evolution']):
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


    def plot_data(self, label=None, title=None, clear_fig=False):
        self.wavelength = self.qepro_dic['QEPro_x_axis']
        self.output = self.qepro_dic['QEPro_output']
        
        if self.stream_name == 'absorbance':
            y_label = 'Absorbance'
            try: f = plt.figure(self.fig[2])
            except (IndexError): f = plt.figure(self.fig[-1])
            plt.clf()
            ax = f.gca()
        
        elif self.stream_name == 'fluorescence':
            y_label = 'Fluorescence'
            try: f = plt.figure(self.fig[3])
            except (IndexError): f = plt.figure(self.fig[-1])
            plt.clf()
            ax = f.gca()
        
        elif self.stream_name == 'primary':
            if self.qepro_dic['QEPro_spectrum_type'] == 3:
                y_label = 'Absorbance'
                try: f = plt.figure(self.fig[0])
                except (IndexError): f = plt.figure(self.fig[-1])
                if clear_fig:
                    plt.clf()
                ax = f.gca()
            else:
                y_label = 'Fluorescence'
                try: f = plt.figure(self.fig[1])
                except (IndexError): f = plt.figure(self.fig[-1])
                if clear_fig:
                    plt.clf()
                ax = f.gca()

        for i in range(self.wavelength.shape[0]):
            if label == None:
                label = f'{self.time}_{i:03d}'
            ax.plot(self.wavelength[i], self.output[i], label=label)
            label = None
        # ax.set_facecolor((0.95, 0.95, 0.95))
        ax.set_xlabel('Wavelength (nm)', fontdict={'size': 14})
        ax.set_ylabel(y_label, fontdict={'size': 14})
        if title == None:
            title = f'{self.date}-{self.time}_{self.uid[0:8]}_{self.stream_name}'
        ax.set_title(title)
        ax.legend()
        f.canvas.manager.show()
        f.canvas.flush_events()


    def plot_peak_fit(self, x, y, fit_function, popt, peak=None, fill_between=False):
        y_label = 'Fluorescence'
        
        try:
            f = plt.figure(self.fig[4])
        except (IndexError):
            f = plt.figure(self.fig[-1])
        
        plt.clf()
        ax = f.gca()
        
        fitted_y = fit_function(x, *popt)
        r_2 = da.r_square(x, y, fitted_y)
        r2 = f'R\u00b2={r_2:.2f}'
        ax.plot(x,y,'b+:',label='data')
        ax.plot(x,fitted_y,'r--',label='Total fit\n'+r2)
        
        try:
            for i in range(len(peak)):
                ax.plot(x[peak[i]], y[peak[i]], '*', markersize=12)
        except (TypeError, IndexError):
            pass
        
        if fill_between:
            if 'gauss' in fit_function.__name__:
                f1 = da._1gauss
                for i in range(int(len(popt)/3)):
                    pars_i = popt[0+3*i:3+3*i]
                    peak_i = f1(x, *pars_i)
                    ax.plot(x, peak_i, label=f'peak {i+1}')
                    ax.fill_between(x, peak_i.min(), peak_i, alpha=0.3)
            else:
                print(f'\n** Plot fill_between for {fit_function.__name__} is not supported. ** \n')

        # ax.set_facecolor((0.95, 0.95, 0.95))
        ax.set_xlabel('Wavelength (nm)', fontdict={'size': 14})
        ax.set_ylabel(y_label, fontdict={'size': 14})
        ax.set_title(f'{self.date}-{self.time}_{self.uid[0:8]}_{self.stream_name}_{fit_function.__name__}')
        ax.legend()
        f.canvas.manager.show()
        f.canvas.flush_events()
        


    def plot_average_good(self, x, y, color=None, label=None):        
        
        # import palettable.colorbrewer.diverging as pld
        # palette = pld.RdYlGn_4_r
        # cmap = palette.mpl_colormap

        y_label = 'Fluorescence'
        
        try:
            f = plt.figure(self.fig[5])
        except (IndexError):
            f = plt.figure(self.fig[-1])
        
        ax = f.gca()
        
        if label == None:
            label = self.time + '_' + self.uid[:8]

        if color == None:
            ax.plot(x, y, label=label)
        else:
            ax.plot(x, y, label=label, color=color)

        # ax.set_facecolor((0.95, 0.95, 0.95))
        ax.set_xlabel('Wavelength (nm)', fontdict={'size': 14})
        ax.set_ylabel(y_label, fontdict={'size': 14})
        # ax.set_title(f'{self.date}-{self.time}_{self.uid[0:8]}_{self.stream_name}_{fit_function.__name__}')
        ax.legend()
        f.canvas.manager.show()
        f.canvas.flush_events()
        

        

class open_subfigures():
    def __init__(self, rows, columns, figsize, ax_titles):
        f1, ax1 = plt.subplots(rows, columns, figsize=figsize, constrained_layout=True)
        ax1 = ax1.flatten()
        for i in range(len(ax_titles)):
            ax1[i].set_title(ax_titles[i], fontsize=10)
        
class multipeak_fitting(open_subfigures):
    def __init__(self, rows=2, columns=2, figsize = (8, 6), ax_titles=['_1gauss', '_2gauss', '_3gauss', 'r_2']):
        super().__init__(rows, columns, figsize, ax_titles)
        self.fig = plt.gcf()
        self.ax = self.fig.get_axes()
        
    def plot_fitting(self, x, y, popt_list, single_f, fill_between=True, num_var=3):
        for i in range(len(popt_list)):
            
            fitted_y = np.zeros([x.shape[0]])
            for j in range(i+1):
                fitted_y += single_f(x, *popt_list[i][0+num_var*j:num_var+num_var*j])
            
            self.ax[i].plot(x ,y, 'b+:',label='data')
            r_2 = da.r_square(x, y, fitted_y, y_low_limit=500)
            r2 = f'R\u00b2={r_2:.2f}'
            self.ax[i].plot(x,fitted_y,'r--',label='Total fit\n'+r2)
            
            if fill_between:
                f1 = single_f
                for k in range(int(len(popt_list[i])/3)):
                    pars_k = popt_list[i][0+3*k:3+3*k]
                    peak_k = f1(x, *pars_k)
                    self.ax[i].plot(x, peak_k, label=f'peak {k+1}')
                    self.ax[i].fill_between(x, peak_k.min(), peak_k, alpha=0.3)
            
            self.ax[i].legend()
            
        
