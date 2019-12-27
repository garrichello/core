"""Provides classes:
    SLDLegend
"""

import logging

import xmltodict
from matplotlib import cm, colors
import numpy as np

from core.base.common import make_raw_filename, make_filename, listify

class SLDLegend:
    """Class SLDLegend.
    Provides methods for creating legend files in SLD format.
    """

    def __init__(self, data_info):
        """Initiates legend class.

        Arguments:
            data_info -- global data description dictionary
        """
        self.logger = logging.getLogger()
        self._data_info = data_info
        self._legend_options = self._data_info['data']['graphics']['legend']

    def _get_meta_value(self, options, value_name):
        """ Returns value in options meta dictionary.

        Arguments:
            options -- dictionary of write options
            value_name -- name of meta value to extract

        Returns:
            value -- value or None if it (or meta) deoe not exist.
        """

        value = None
        if options.get('meta') is not None:
            if options['meta'].get(value_name) is not None:
                value = options['meta'][value_name]

        return value

    def _make_legend_values(self, values, options):
        """Prepares legend values.

        Arguments:
            values -- processing result's values as a masked array/array/list
            options -- dictionary of write options

        Returns:
            legend_values, legend_labels, rgb_values -- legend values, tabels and RGB-mapping
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

        # Check for NaN
        if np.isnan(data_min) or np.isnan(data_max):
            self.logger.error('Values array contains NaNs. But should not. Something wrong. Can\'t continue...')
            raise ValueError

        # Check for optional color values and names
        legend_override = None
        if options.get('meta') is not None:
            if options['meta'].get('legend_override') is not None:
                legend_override = options['meta']['legend_override']

        # Generate legend colors, labels and values.
        if legend_override is not None:
            num_colors = len(legend_override)
        else:
            num_colors = 253 if self._legend_options['@type'] == 'continuous' else int(self._legend_options['ncolors'])
        legend_colors = [int(float(color_idx) / (num_colors) * 253.0) for color_idx in range(num_colors + 1)]
        legend_colors.reverse()
        if legend_override is not None:
            num_labels = num_colors
        else:
            num_labels = int(self._legend_options['nlabels'])
        # Color's indices to be shown in the legend.
        idxs = [int(idx * (num_colors - 1 + 0.9999) / (num_labels - 1)) for idx in range(num_labels)]

        # Generate format string for printing legend labels according to a difference between maximum and minimum values
        if values.count() != 0:  # When data values are present
            if legend_override:
                legend_values = list(legend_override.keys())
                legend_labels = list(legend_override.values())
            else:
                if values.dtype == np.dtype('bool'):
                    legend_values = [1, 0]
                    legend_labels = ['Unmasked', 'Masked']
                else:
                    legend_values = [(legend_colors[i]) / float(num_colors) * (data_max - data_min) + data_min for i in idxs]
                    # Order of magnitude of the difference between max and min values.
                    if data_max != data_min:
                        value_order = np.log10((data_max - data_min) / num_labels)
                    else:
                        value_order = np.log10((data_max) / num_labels)
                    precision = 0 if value_order >= 0 else int(np.ceil(abs(value_order)))
                    width = int(np.ceil(np.log10(data_max)) + precision)
                    format_string = '{}:{}.{}f{}'.format('{', width, precision, '}')
                    # Labels for each colorbar tick
                    legend_labels = [format_string.format(value) + ' ' + options['description']['@units'] for value in legend_values]
            # Colors for each colorbar tick in HEX format
            rgb_values = [colors.to_hex(colormap(legend_colors[i])) for i in idxs]
        else:   # When there are no any data values
            legend_values = [values.fill_value for i in idxs]
            legend_labels = ['NO DATA!' for value in legend_values]  # Labels for each colorbar tick
            rgb_values = [colors.to_hex((0, 0, 0)) for i in idxs]  # Colors for each colorbar tick in HEX format

        return legend_values, legend_labels, rgb_values

    def write(self, all_values, all_options):
        """Writes a legend in an SLD file. Supports multiple bands (variables).

        Arguments:
            all_values -- processing result's values as a masked array/array/list (list of arrays in multiband case)
            all_options -- dictionary of write options (list of dictionaries in multiband case)
        """

        self.logger.info('Writing legend...')

        # Prepare legend values.
        legend_properties = []
        for values, options in zip(listify(all_values), listify(all_options)):
            cur_leg_val, cur_leg_lab, cur_rgb_val = self._make_legend_values(values, options)
            legend_data = {}
            legend_data['values'] = cur_leg_val
            legend_data['labels'] = cur_leg_lab
            legend_data['rgb'] = cur_rgb_val
            legend_data['layer_name'] = options['description']['@name']
            legend_data['fill_value'] = values.fill_value
            legend_properties.append(legend_data)

        # Use legend options to make legend filename(s).
        legend_info = {'data': self._legend_options}

        # Select writer corresponding to data kind.
        if self._legend_options['@data_kind'] == 'raster':
            legend_filename = make_raw_filename(legend_info, all_options)
            self.write_raster(legend_filename, legend_properties)
        if self._legend_options['@data_kind'] == 'station':
            for legend_data in legend_properties:
                legend_filename = make_filename(legend_info, all_options)
                self.write_stations(legend_filename, legend_data)

        self.logger.info('Done!')

    def write_raster(self, filename: str, legend_properties: list):
        """Writes a legend for raster data into SLD file. Supports multiple bands (variables).

        Arguments:
            filename -- name of SLD file.
            legend_properties -- list of legend properties, each element is a dict:
                'values' -- data values to be written into SLD file.
                'labels' -- labels corresponding to data values.
                'rgb' -- hex-values of colors corresponding to data values.
                'layer_name' -- name of the raster layer.
                'fill_value' -- values for transparent pixels
        """

        sld = {}
        sld['StyledLayerDescriptor'] = {}
        sld['StyledLayerDescriptor']['@version'] = '1.0.0'
        sld['StyledLayerDescriptor']['@xmlns'] = 'http://www.opengis.net/sld'
        sld['StyledLayerDescriptor']['@xmlns:ogc'] = 'http://www.opengis.net/ogc'
        sld['StyledLayerDescriptor']['@xmlns:xlink'] = 'http://www.w3.org/1999/xlink'
        sld['StyledLayerDescriptor']['@xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
        sld['StyledLayerDescriptor']['@xsi:schemaLocation'] = \
            'http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd'

        sld['StyledLayerDescriptor']['NamedLayer'] = []
        for channel, legend_data in enumerate(legend_properties):
            named_layer_entry = {}
            named_layer_entry['Name'] = legend_data['layer_name']
            named_layer_entry['UserStyle'] = {}
            named_layer_entry['UserStyle']['Name'] = 'Raster'
            named_layer_entry['UserStyle']['Title'] = 'Raster'
            named_layer_entry['UserStyle']['Abstract'] = 'Colors'

            feature_type_style = {}
            feature_type_style['Rule'] = {}
            feature_type_style['Rule']['RasterSymbolizer'] = {}
            feature_type_style['Rule']['RasterSymbolizer']['Opacity'] = 0.8
            colormap_entry = []
            if legend_data['fill_value'].dtype != np.dtype('bool'):  # Transparent values are NOT bool!
                colormap_entry.append({'@color': '#000000',
                                       '@quantity': legend_data['fill_value'],
                                       '@label': '',
                                       '@opacity': '0.0'})
            for i, _ in reversed(list(enumerate(legend_data['values']))):
                # Missing values are transparent
                entry_opacity = 0.0 if legend_data['values'][i] == legend_data['fill_value'] else 1.0
                colormap_entry.append({'@color': legend_data['rgb'][i],
                                       '@quantity': legend_data['values'][i],
                                       '@label': legend_data['labels'][i],
                                       '@opacity': entry_opacity})
            channel_selection = {}
            channel_selection['GrayChannel'] = {}
            channel_selection['GrayChannel']['SourceChannelName'] = channel
            feature_type_style['Rule']['RasterSymbolizer']['ColorMap'] = {}
            feature_type_style['Rule']['RasterSymbolizer']['ColorMap']['ColorMapEntry'] = colormap_entry
            feature_type_style['Rule']['RasterSymbolizer']['ChannelSelection'] = channel_selection
            named_layer_entry['UserStyle']['FeatureTypeStyle'] = feature_type_style
            sld['StyledLayerDescriptor']['NamedLayer'].append(named_layer_entry)

        with open(filename, 'w') as file_descriptor:
            file_descriptor.write(xmltodict.unparse(sld, pretty=True))

    def write_stations(self, filename: str, legend_data: dict):
        """Writes a legend for station data into SLD file.

        Arguments:
            filename -- name of SLD file.
            legend_data -- dictionary with legend data:
                'values' -- data values to be written into SLD file.
                'labels' -- labels corresponding to data values.
                'rgb' -- hex-values of colors corresponding to data values.
                'layer_name' -- name of the named layer.
                'fill_value' -- values for transparent pixels
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
        sld['StyledLayerDescriptor']['@xsi:schemaLocation'] = \
            'http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd'
        sld['StyledLayerDescriptor']['NamedLayer'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['Name'] = 'Stations'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['Title'] = 'Station data'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['Abstract'] = 'Based on VALUE field'
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle'] = {}
        sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule'] = []

        n_values = len(legend_data['values'])
        for i in range(n_values-1, -1, -1):
            rule = {}
            ogc_filter = {}
            if i == n_values-1: # Top line of the legend
                rule['Name'] = 'First level'
                rule['Title'] = '<' + legend_data['labels'][i]
                ogc_filter['ogc:PropertyIsLessThan'] = {}
                ogc_filter['ogc:PropertyIsLessThan']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:PropertyIsLessThan']['ogc:Literal'] = legend_data['values'][i]
            elif i == 0: # Bottom line of the legend
                rule['Name'] = 'Last level'
                rule['Title'] = '>' + legend_data['labels'][i]
                ogc_filter['ogc:PropertyIsGreaterThanOrEqualTo'] = {}
                ogc_filter['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:Literal'] = legend_data['values'][i]
            else:
                rule['Name'] = 'Next level'
                rule['Title'] = legend_data['labels'][i] + ' .. ' + legend_data['labels'][i-1]
                ogc_filter['ogc:And'] = {}
                ogc_filter['ogc:And']['ogc:PropertyIsGreaterThanOrEqualTo'] = {}
                ogc_filter['ogc:And']['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:And']['ogc:PropertyIsGreaterThanOrEqualTo']['ogc:Literal'] = legend_data['values'][i]
                ogc_filter['ogc:And']['ogc:PropertyIsLessThan'] = {}
                ogc_filter['ogc:And']['ogc:PropertyIsLessThan']['ogc:PropertyName'] = field_name
                ogc_filter['ogc:And']['ogc:PropertyIsLessThan']['ogc:Literal'] = legend_data['values'][i-1]

            rule['ogc:Filter'] = ogc_filter
            point_symbolizer = {}
            point_symbolizer['Graphic'] = {}
            point_symbolizer['Graphic']['Mark'] = {}
            point_symbolizer['Graphic']['Mark']['WellKnownName'] = symbol_name
            point_symbolizer['Graphic']['Mark']['Fill'] = {}
            point_symbolizer['Graphic']['Mark']['Fill']['CssParameter'] = {}
            point_symbolizer['Graphic']['Mark']['Fill']['CssParameter']['@name'] = 'fill'
            point_symbolizer['Graphic']['Mark']['Fill']['CssParameter']['#text'] = legend_data['rgb'][i]
            point_symbolizer['Graphic']['Mark']['Stroke'] = {}
            point_symbolizer['Graphic']['Mark']['Stroke']['CssParameter'] = []
            point_symbolizer['Graphic']['Mark']['Stroke']['CssParameter'].append({'@name':'stroke', '#text':legend_data['rgb'][i]})
            point_symbolizer['Graphic']['Mark']['Stroke']['CssParameter'].append({'@name':'stroke-width', '#text':'1'})
            point_symbolizer['Graphic']['Size'] = symbol_size
            rule['PointSymbolizer'] = point_symbolizer
            sld['StyledLayerDescriptor']['NamedLayer']['UserStyle']['FeatureTypeStyle']['Rule'].append(rule)

        with open(filename, 'w') as file_descriptor:
            file_descriptor.write(xmltodict.unparse(sld, pretty=True, indent='    '))
