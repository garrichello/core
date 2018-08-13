"""Provides classes:
    SLDLegend
"""

import os.path

import xmltodict
from matplotlib import cm, colors
import numpy as np

class SLDLegend:
    """Class SLDLegend.
    Provides methods for creating legend files in SLD format.
    """

    def __init__(self, data_info):
        """Initiates legend class.

        Arguments:
            data_info -- global data description dictionary
        """
        self._data_info = data_info
        self._legend_options = self._data_info['data']['graphics']['legend']

    def write(self, values, options):
        """Writes a legend in an SLD file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options
        """

        # Get RGB values of the selected color map.
        colormap = cm.get_cmap(self._data_info['data']['graphics']['colortable'].lower())

        # Determine min and max values.
        if self._legend_options['limited'] == 'yes':
            data_min = float(legend_options['minimum'])
            data_max = float(legend_options['maximum'])
        else:
            data_min = values.min()
            data_max = values.max()

        # Generate legend colors, labels and values.
        num_colors = 253 if self._legend_options['@type'] == 'continuous' else int(self._legend_options['ncolors'])
        legend_colors = [int(color_idx/num_colors*253) for color_idx in range(num_colors+1)]
        legend_colors.reverse()
        num_labels = int(self._legend_options['nlabels'])
        idxs = [int(idx*(num_colors-1+0.9999)/(num_labels-1)) for idx in range(num_labels)] # Color's indices to be shown in the legend.
        legend_values = [(legend_colors[i]-1)/float(num_colors-1)*(data_max-data_min)+data_min for i in idxs]
        
        # Generate format string for printing legend labels according to a difference between maximum and minimum values
        value_order = np.log10((data_max-data_min)/num_labels) # Order of magnitude of the difference between max and min values.
        precision = 0 if value_order >= 0 else int(np.ceil(abs(value_order)))
        width = int(np.ceil(np.log10(data_max)) + precision)
        format_string = '{}:{}.{}f{}'.format('{', width, precision, '}')
        
        legend_labels = [format_string.format(value) for value in legend_values] # Labels for each colorbar tick
        rgb_values = [colors.to_hex(colormap(legend_colors[i])) for i in idxs] # Colors for each colorbar tick in HEX format

        # Legend file name composition.
        (file_root, file_ext) = os.path.splitext(self._legend_options['file']['@name'])
        legend_filename = '{}_{}_{}-{}{}'.format(file_root, options['level'], 
            options['segment']['@beginning'], options['segment']['@ending'], file_ext)

        # Select writer corresponding to data kind.
        if self._legend_options['@data_kind'] == 'raster':
            self.write_raster(legend_filename, legend_values, legend_labels, rgb_values, values.fill_value)

    def write_raster(self, filename, legend_values, legend_labels, rgb_values, fill_value):
        """Writes a legend for raster data into SLD file.

        Arguments:
            filename -- name of SLD file.
            legend_values -- data values to be written into SLD file.
            legend_labels -- labels corresponding to data values.
            rgb_values -- hex-values of colors corresponding to data values.
            fill_value -- value for transparent pixels
        """
        sld = {}
        sld['StyledLayerDescriptor'] = {}
        sld['StyledLayerDescriptor']['@version'] = '1.0.0'
        sld['StyledLayerDescriptor']['@xmlns'] = 'http://www.opengis.net/sld'
        sld['StyledLayerDescriptor']['@xmlns:ogc'] = 'http://www.opengis.net/ogc'
        sld['StyledLayerDescriptor']['@xmlns:xlink'] = 'http://www.w3.org/1999/xlink'
        sld['StyledLayerDescriptor']['@xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
        sld['StyledLayerDescriptor']['@xsi:schemaLocation'] = 'http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd'
        sld['StyledLayerDescriptor']['NamedLayer'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['Name'] = 'Raster Layer'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['Name'] = 'Raster'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['Title'] = 'Raster'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['Abstract'] = 'Colors'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule']['RasterSymbolizer'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule']['RasterSymbolizer']['Opacity'] = 0.8
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule']['RasterSymbolizer']['ColorMap'] = {}
        colormap_entry = [{'@color':'#000000', '@quantity':fill_value, '@label':'', '@opacity':'0.0'}]
        for i in range(len(legend_values)):
            colormap_entry.append({'@color':rgb_values[i], '@quantity': legend_values[i], '@label':legend_labels[i]})
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule']['RasterSymbolizer']['ColorMap']['ColorMapEntry'] = colormap_entry
    
        with open(filename, 'w') as fd:
            fd.write(xmltodict.unparse(sld, pretty=True))