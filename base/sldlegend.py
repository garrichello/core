"""Provides classes:
    SLDLegend
"""

import xmltodict
from matplotlib import cm, colors
import numpy as np

class SLDLegend:
    """Class SLDLegend.
    Provides methods for creating legend files in SLD format.
    """

    def __init__(self, legend_options, colormap = 'rainbow', data_min = None, data_max = None, fill_value = None):
        """Initiates legend class.

        Arguments:
            legend_options -- dictionary describing legend according to a task file
            data_min -- minimum data value
            data_max -- maximum data value
        """
        self._legend_options = legend_options
        self._colormap = colormap
        self._data_min = data_min if data_min is not None else float(legend_options['minimum'])
        self._data_max = data_max if data_max is not None else float(legend_options['maximum'])
        self._fill_value = fill_value if fill_value is not None else -999.0

    def write(self):
        """Writes a legend in an SLD file.
        """
        filename = self._legend_options['file']['@name']

        # Get RGB values of the selected color map.
        colormap = cm.get_cmap(self._colormap)

        # Generate legend colors, values and labels
        num_colors = 253 if self._legend_options['@type'] == 'continuous' else int(self._legend_options['ncolors'])
        legend_colors = [int(color_idx/num_colors*253) for color_idx in range(num_colors+1)]
        legend_colors.reverse()
        num_labels = int(self._legend_options['nlabels'])
        idxs = [int(idx*(num_colors-1+0.9999)/(num_labels-1)) for idx in range(num_labels)]
        legend_values = [(legend_colors[i]-1)/float(num_colors-1)*(self._data_max-self._data_min)+self._data_min for i in idxs]
        value_order = np.log10((self._data_max-self._data_min)/num_labels)
        precision = 0 if value_order >= 0 else int(abs(np.ceil(value_order)))
        width = int(np.ceil(np.log10(self._data_max)) + precision)
        format_string = '{}:{}.{}f{}'.format('{', width, precision, '}')
        legend_labels = [format_string.format(value) for value in legend_values]
        rgb_values = [colors.to_hex(colormap(legend_colors[i])) for i in idxs] # Colors in HEX format

        # Select writer corresponding to data kind.
        if self._legend_options['@data_kind'] == 'raster':
            self.write_raster(filename, legend_values, legend_labels, rgb_values)

    def write_raster(self, filename, legend_values, legend_labels, rgb_values):
        pass