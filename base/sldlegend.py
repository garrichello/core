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
            data_min = float(self._legend_options['minimum'])
            data_max = float(self._legend_options['maximum'])
        else:
            data_min = values.min()
            data_max = values.max()

        # Check for optional color values and names
        values_override = None
        if options.get('meta') is not None:
            if options['meta'].get('levels') is not None:
                values_override = options['meta']['levels']

        # Generate legend colors, labels and values.
        if values_override is not None:
            num_colors = len(values_override)
        else:
            num_colors = 253 if self._legend_options['@type'] == 'continuous' else int(self._legend_options['ncolors'])
        legend_colors = [int(float(color_idx) / (num_colors) * 253.0) for color_idx in range(num_colors + 1)]
        legend_colors.reverse()
        if values_override is not None:
            num_labels = num_colors
        else:
            num_labels = int(self._legend_options['nlabels'])
        # Color's indices to be shown in the legend.
        idxs = [int(idx * (num_colors - 1 + 0.9999) / (num_labels - 1)) for idx in range(num_labels)]

        # Generate format string for printing legend labels according to a difference between maximum and minimum values
        if values.count() != 0:  # When data values are present
            if values_override:
                legend_values = list(values_override.keys())
                legend_labels = list(values_override.values())
            else:
                legend_values = [(legend_colors[i]) / float(num_colors) * (data_max - data_min) + data_min for i in idxs]
                # Order of magnitude of the difference between max and min values.
                value_order = np.log10((data_max - data_min) / num_labels)
                precision = 0 if value_order >= 0 else int(np.ceil(abs(value_order)))
                width = int(np.ceil(np.log10(data_max)) + precision)
                format_string = '{}:{}.{}f{}'.format('{', width, precision, '}')
                # Labels for each colorbar tick
                legend_labels = ' '.join([[format_string.format(value) for value in legend_values], 
                                          options['description']['@units']])
            # Colors for each colorbar tick in HEX format
            rgb_values = [colors.to_hex(colormap(legend_colors[i])) for i in idxs]
        else:   # When there are no any data values
            legend_values = [values.fill_value for i in idxs]
            legend_labels = ['NO DATA!' for value in legend_values]  # Labels for each colorbar tick
            rgb_values = [colors.to_hex((0, 0, 0)) for i in idxs]  # Colors for each colorbar tick in HEX format

        # Legend file name composition.
        (file_root, file_ext) = os.path.splitext(self._legend_options['file']['@name'])
        legend_filename = \
            '{}_{}_{}-{}{}'.format(file_root, options['level'], options['segment']['@beginning'],
                                   options['segment']['@ending'], file_ext)

        # Select writer corresponding to data kind.
        if self._legend_options['@data_kind'] == 'raster':
            self.write_raster(legend_filename, legend_values, legend_labels, rgb_values, values.fill_value)
        if self._legend_options['@data_kind'] == 'station':
            self.write_stations(legend_filename, legend_values, legend_labels, rgb_values)

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
        for i, _ in enumerate(legend_values):
            colormap_entry.append({'@color':rgb_values[i], '@quantity': legend_values[i], '@label':legend_labels[i]})
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule']['RasterSymbolizer']['ColorMap']['ColorMapEntry'] = colormap_entry

        with open(filename, 'w') as file_descriptor:
            file_descriptor.write(xmltodict.unparse(sld, pretty=True))

    def write_stations(self, filename, legend_values, legend_labels, rgb_values):
        """Writes a legend for stations data into SLD file.

        Arguments:
            filename -- name of SLD file.
            legend_values -- data values to be written into SLD file.
            legend_labels -- labels corresponding to data values.
            rgb_values -- hex-values of colors corresponding to data values.
        """

        field_name = 'VALUE'
        symbol_name = 'circle'
        symbol_size = 6

        sld = {}
        sld['StyledLayerDescriptor'] = {}
        sld['StyledLayerDescriptor']['@version'] = '1.0.0'
        sld['StyledLayerDescriptor']['@xmlns'] = 'http://www.opengis.net/sld'
        sld['StyledLayerDescriptor']['@xmlns:ogc'] = 'http://www.opengis.net/ogc'
        sld['StyledLayerDescriptor']['@xmlns:xlink'] = 'http://www.w3.org/1999/xlink'
        sld['StyledLayerDescriptor']['@xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
        sld['StyledLayerDescriptor']['@xsi:schemaLocation'] = 'http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd'
        sld['StyledLayerDescriptor']['NamedLayer'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['Name'] = 'Stations'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['Title'] = 'Station data'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['Abstract'] = 'Based on VALUE field'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule'] = []

        n_values = len(legend_values)
        for i in range(n_values-1, -1, -1):
            rule = {}
            ogc_filter = {}
            if i == n_values-1: # Top line of the legend
                rule['Name'] = 'First level'
                rule['Title'] = '<'+legend_labels[i]
                ogc_filter['ogc:PropertyIsLessThan'] = {}
                ogc_filter['ogc:PropertyIsLessThan']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:PropertyIsLessThan']['ogc:Literal'] = legend_values[i]
            elif i == 0: # Bottom line of the legend
                rule['Name'] = 'Last level'
                rule['Title'] = '>'+legend_labels[i]
                ogc_filter['ogc:PropertyIsGreaterThanOrEqualTo'] = {}
                ogc_filter['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:Literal'] = legend_values[i]
            else:
                rule['Name'] = 'Next level'
                rule['Title'] = legend_labels[i] + ' .. ' + legend_labels[i-1]
                ogc_filter['ogc:And'] = {}
                ogc_filter['ogc:And']['ogc:PropertyIsGreaterThanOrEqualTo'] = {}
                ogc_filter['ogc:And']['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:And']['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:Literal'] = legend_values[i]
                ogc_filter['ogc:And']['ogc:PropertyIsLessThan'] = {}
                ogc_filter['ogc:And']['ogc:PropertyIsLessThan']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:And']['ogc:PropertyIsLessThan']['ogc:Literal'] = legend_values[i-1]

            rule['ogc:Filter'] = ogc_filter
            point_symbolizer = {}
            point_symbolizer['Graphic'] = {}
            point_symbolizer['Graphic']['Mark'] = {}
            point_symbolizer['Graphic']['Mark']['WellKnownName'] = symbol_name
            point_symbolizer['Graphic']['Mark']['Fill'] = {}
            point_symbolizer['Graphic']['Mark']['Fill']['CssParameter'] = {}
            point_symbolizer['Graphic']['Mark']['Fill']['CssParameter']['@name'] = 'fill'
            point_symbolizer['Graphic']['Mark']['Fill']['CssParameter']['#text'] = rgb_values[i]
            point_symbolizer['Graphic']['Mark']['Stroke'] = {}
            point_symbolizer['Graphic']['Mark']['Stroke']['CssParameter'] = []
            point_symbolizer['Graphic']['Mark']['Stroke']['CssParameter'].append({'@name':'stroke', '#text':rgb_values[i]})
            point_symbolizer['Graphic']['Mark']['Stroke']['CssParameter'].append({'@name':'stroke-width', '#text':'1'})
            point_symbolizer['Graphic']['Size'] = symbol_size
            rule['PointSymbolizer'] = point_symbolizer
            sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule'].append(rule)

        with open(filename, 'w') as file_descriptor:
            file_descriptor.write(xmltodict.unparse(sld, pretty=True, indent='    '))
