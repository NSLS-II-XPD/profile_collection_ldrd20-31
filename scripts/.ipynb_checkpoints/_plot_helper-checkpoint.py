import matplotlib.pyplot as plt
import numpy
from side_functions import _readable_time


class open_figures():
    def __init__(self, figure_labels):
        for i in figure_labels:
            plt.figure(num=i)


class plot_uvvis(open_figures):
    
    def __init__(self, qepro_dic, metadata_dic, 
                 figure_labels = ['primary_absorbance', 'primary_fluorescence', 
                                  'bundle_absorbance', 'bundle_fluorescence'
                                  'fluorescence oeak fitting']):
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
            f = plt.figure(self.fig[2])
            ax = f.gca()
        
        elif self.stream_name == 'fluorescence':
            y_label = 'Fluorescence'
            f = plt.figure(self.fig[3])
            ax = f.gca()
        
        elif self.stream_name == 'primary':
            if self.qepro_dic['QEPro_spectrum_type'] == 3:
                y_label = 'Absorbance'
                f = plt.figure(self.fig[0])
                ax = f.gca()
            else:
                y_label = 'Fluorescence'
                f = plt.figure(self.fig[1])
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


    def plot_analysis(self, x, y, peak, prop, popt, r_2):
        y_label = 'Fluorescence'
        f = plt.figure(self.fig[4])
        ax = f.gca()

        if len(peak) == 1:
            # ax.plot(self.wavelength[i], self.output[i], label=f'{self.time}_{i:03d}')
            ax.plot(x,y,'b+:',label='data')
            ax.plot(x,fitted_result,'ro:',label='Total fit\n'+r2)

        # ax.set_facecolor((0.95, 0.95, 0.95))
        ax.set_xlabel('Wavelength (nm)', fontdict={'size': 14})
        ax.set_ylabel(y_label, fontdict={'size': 14})
        ax.set_title(f'{self.date}-{self.time}_{self.uid[0:8]}_{self.stream_name}')
        ax.legend()
        f.canvas.manager.show()
        f.canvas.flush_events()