import matplotlib
import matplotlib.pyplot as plt
import numpy
import matplotlib.gridspec as gridspec
from side_functions import _readable_time

class plot_uvvis():
    
    def __init__(self, uid, stream_name, qepro_dic, metadata_dic, fig = None):
        self.fig = fig
        self.uid = metadata_dic['uid']
        self.stream_name = stream_name
        self.qepro_dic = qepro_dic
        self.metadata_dic = metadata_dic
        self.wavelength = []
        self.output = []   
        # self.num = None
        self.date, self.time = _readable_time(metadata_dic['time'])


    def plot_data(self):
        self.wavelength = self.qepro_dic['QEPro_x_axis']
        
        if self.stream_name == 'absorbance':
            self.output = self.qepro_dic['QEPro_output']
            y_label = 'Absorbance'
        
        elif self.stream_name == 'fluorescence':
            self.output = self.qepro_dic['QEPro_output']
            y_label = 'Fluorescence'
        
        elif self.stream_name == 'primary':
            if self.qepro_dic['QEPro_spectrum_type'] == 3:
                self.output = self.qepro_dic['QEPro_output']
                y_label = 'Absorbance'
            else:
                self.output = self.qepro_dic['QEPro_output']
                y_label = 'Fluorescence'

        
        if self.fig is not None:
            plt.close(self.fig.number)
        self.fig = plt.figure()
        ax = self.fig.gca()
        # ax.scatter(xaxis, data, color='blue')
        for i in range(self.wavelength.shape[0]):
            ax.plot(self.wavelength[i], self.output[i], label=f'{i:03d}')
        # ax.set_facecolor((0.95, 0.95, 0.95))
        ax.set_xlabel('Wavelength (nm)', fontdict={'size': 14})
        ax.set_ylabel(y_label, fontdict={'size': 14})
        ax.set_title(f'{self.date}-{self.time}_{self.uid[0:8]}_{self.stream_name}')
        ax.legend()
        self.fig.canvas.manager.show()
        self.fig.canvas.flush_events()
