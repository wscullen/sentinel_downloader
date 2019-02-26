## Script for pre-processing S1 and RS2 DUAL POL SAR Data using Snappy
## Source code: http://remote-sensing.eu/preprocessing-of-sentinel-1-sar-data-via-snappy-python-module/

# Built-in libraries
import time
import os
from pathlib import Path
import datetime

# Third party library
# http://step.esa.int/docs/v4.0/apidoc/engine/org/esa/snap/core/
import snappy
from snappy import GPF
from snappy import jpy
from snappy import ProductIO

class S1_Preprocessor():

    def __init__(self, product, wkt_footprint, working_dir, external_dem_dir):
        """Prepare to use SNAPPY as a subprocess

        This class prepares commands and runs SNAPPY as a python3.4 subprocess

        """

        self.working_dir = working_dir
        self.product_meta = product
        self.wkt_footprint = wkt_footprint
        self.dem_dir = external_dem_dir

        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

        # Set object parameterisation dictionary
        self.HashMap = jpy.get_type('java.util.HashMap')

        # Set start time and loop counter
        self.start_time = datetime.datetime.now()  # for calculation
        self.start_time_string = self.start_time.strftime("%Y-%m-%d %H:%M:%S")

        print("Processing start time:", self.start_time)
        self.name = self.product_meta['name']

        self.name_with_safe = self.name + '.SAFE'

        self.product_path = os.path.join(self.working_dir, self.name + '.zip')
        self.preprocess_path = working_dir
        self.manifest_path = os.path.join(self.working_dir, self.name_with_safe, 'manifest.safe')

        # for rs2
        self.productxml_path = os.path.join(self.working_dir, self.name, 'product.xml')

        # Read S1 raw data product and assigns source bands for given file
        # readProduct(File file, String... formatNames)
        if product['name'].startswith("S1"):
            self.dataproduct_r = snappy.ProductIO.readProduct(self.manifest_path)
            # self.srcbands = ["Intensity_VV", "Intensity_VH", "Amplitude_VV", "Amplitude_VH"]
            self.srcbands = ["Intensity_VV", "Intensity_VH"]

            print("Reading S1 data product with polarization bands:", self.srcbands)
        # Read RS2 raw data product and assigns source bands for given file
        else:
            self.dataproduct_r = snappy.ProductIO.readProduct(self.productxml_path)
            # self.srcbands = ["Intensity_VV", "Intensity_VH", "Amplitude_VV", "Amplitude_VH"]
            print("Reading RS2 data product with polarization bands:", self.srcbands)

        self.intermediate_product = None

        self.operations_list = []

    def apply_orbit_file(self,
                         write_intermediate=False):

        parameters = self.HashMap()

        dataproduct = GPF.createProduct("Apply-Orbit-File", parameters, self.dataproduct_r)

        self.operations_list.append('ApplyOrbit')

        SF_filepath = ""

        # Get a progressMonitor object
        monitor = self.createProgressMonitor()

        if write_intermediate:
            # Write data product with BEAM-DIM format to given file path
            # ProductIO.writeProduct(Product product, String filePath, String formatName)
            SF_filepath = Path(self.working_dir, self.name_with_safe + "_ORB")
            print('Writing out Orbit File corrected product')

            ProductIO.writeProduct(dataproduct, str(SF_filepath), 'BEAM-DIMAP', monitor)
             # Set start time and loop counter
            finish_time = datetime.datetime.now()  # for calculation
            elapsed_time = finish_time - self.start_time
            print(elapsed_time.strftime("%H:%M:%S"))


        if os.path.exists(SF_filepath + ".dim"):
            print("Completed applying orbit file:", SF_filepath)
        else:
            print("Completed applying orbit file: data product saved to in-memory")

        self.intermediate_product = dataproduct

    def sigma0_conversion(self,
                          write_intermediate=False):

        print('Applying sigma0 convertion...')

        sigma0_parameters = self.HashMap()

        sigma0_parameters.put('sourceBandNames', "Amplitude_VV,Amplitude_VH")

        sigma0_converted = GPF.createProduct("Calibration", sigma0_parameters, self.intermediate_product)

        self.operations_list.append('Sigma0Calibration')

        SF_filepath = ""

        # Get a progressMonitor object
        monitor = self.createProgressMonitor()

        if write_intermediate:
            # Write data product with BEAM-DIM format to given file path
            # ProductIO.writeProduct(Product product, String filePath, String formatName)
            SF_filepath = Path(self.working_dir, self.name_with_safe + "_ORB")

            print('Writing out Sigma0 converted Product')

            ProductIO.writeProduct(sigma0_converted, str(SF_filepath), 'BEAM-DIMAP', monitor)
            finish_time = datetime.datetime.now()  # for calculation
            elapsed_time = finish_time - self.start_time
            print(elapsed_time.strftime("%H:%M:%S"))

        if os.path.exists(SF_filepath + ".dim"):
            print("Completed applying orbit file:", SF_filepath)
        else:
            print("Completed applying orbit file: data product saved to in-memory")

        self.intermediate_product = sigma0_converted


    def speckle_filter(self,
                       filter="Gamma Map",
                       estimateENL=True,
                       filterSize=11,
                       numLooksStr="1",
                       targetWindowStr="3x3",
                       windowSize="11x11",
                       write_intermediate=False):
        print("-------------")

        # Apply SPECKLE FILTER
        print("Applying speckle-filter:", self.name)

        # Define object parameterisation for filtering
        parameters = self.HashMap()
        parameters.put('filter', filter)
        parameters.put('estimateENL', estimateENL)
        parameters.put('filterSizeX', filterSize)
        parameters.put('filterSizeY', filterSize)
        parameters.put('numLooksStr', numLooksStr)
        parameters.put('sourceBands', ",".join(self.srcbands))
        parameters.put('targetWindowStr', targetWindowStr)
        parameters.put('windowSize', windowSize)

        print("Speckle filtering parameters:", parameters)

        # Create speckle filter data product using 'Speckle-Filter' operator/parameters and raw data product as source product
        # Speckle noise reduction can be applied either by spatial filtering or multilook processing
        # Filtered product contains 4 real bands: 2 amplitude and intensity bands for each polarizations
        # createProduct(String operatorName, Map<String,Object> parameters, Map<String,Product> sourceProducts)
        dataproduct = GPF.createProduct("Speckle-Filter", parameters, self.dataproduct_r)

        self.operations_list.append('SpeckleFilter')

        SF_filepath = ""
        # Get a progressMonitor object
        monitor = self.createProgressMonitor()

        if write_intermediate:
            # Write data product with BEAM-DIM format to given file path
            # ProductIO.writeProduct(Product product, String filePath, String formatName)
            SF_filepath = Path(self.working_dir, self.name_with_safe + "_SF")
            print('Writing out SpeckleFilter Product')


            ProductIO.writeProduct(dataproduct, str(SF_filepath), 'BEAM-DIMAP', monitor)
            finish_time = datetime.datetime.now()  # for calculation
            elapsed_time = finish_time - self.start_time
            print(elapsed_time.strftime("%H:%M:%S"))


        if os.path.exists(SF_filepath + ".dim"):
            print("Completed speckle-filtering:", SF_filepath)
        else:
            print("Completed speckle-filtering: data product saved to in-memory")

        self.intermediate_product = dataproduct

    def createProgressMonitor(self):
        PWPM = jpy.get_type('com.bc.ceres.core.PrintWriterProgressMonitor')
        JavaSystem = jpy.get_type('java.lang.System')
        monitor = PWPM(JavaSystem.out)
        return monitor

    def range_doppler_to_sigma0(self, resolution=10.0, suffix="", window_size=11, write_intermediate=False):
           # APPLY Range Doppler terrain correction and ortho

        print('Applying applying R.Doppler terrain corr. and ortho...')

        rd_parameters = self.HashMap()
        rd_parameters.put('demName', "SRTM 1Sec HGT") # Should eventually look at using a local SRTM 1Sec DEM (instead of auto downloading)
        rd_parameters.put('imgResamplingMethod', "BILINEAR_INTERPOLATION")
        rd_parameters.put('pixelSpacingInMeter', resolution)
        rd_parameters.put('sourceBands', ",".join(self.srcbands))

        # rd_parameters.put('mapProjection', projection) # Defined above, should be auto set to best UTM zone

        # APPLY RADIOMETRIC NORMALIZATION to SIGMA0 (not clear if necessary) only do if sigma0 NOT done above
        rd_parameters.put('applyRadiometricNormalization', True)
        rd_parameters.put('incidenceAngleForSigma0', "Use local incidence angle from DEM")
        rd_parameters.put('saveSigmaNought', True)
        rd_parameters.put('saveLocalIncidenceAngle', True)

        rd_corrected = GPF.createProduct("Terrain-Correction", rd_parameters, self.intermediate_product)

        self.operations_list.append("RangeDopplerOrthoConversionToSigma0")

        print(self.name_with_safe)
        final_output_name = self.name_with_safe + "_{}_{}x{}_{}m".format(suffix, window_size, window_size, resolution)
        print('writing out final result')

        # Get a progressMonitor object
        monitor = self.createProgressMonitor()

        SF_filepath = ""

        if write_intermediate:
            # Write data product with BEAM-DIM format to given file path
            # ProductIO.writeProduct(Product product, String filePath, String formatName)
            SF_filepath = Path(self.working_dir, self.name_with_safe + "_ORTHO")
            print('Writing out Terrain-Correction Product')
            ProductIO.writeProduct(rd_corrected, str(SF_filepath), 'BEAM-DIMAP', monitor)
            finish_time = datetime.datetime.now()  # for calculation
            elapsed_time = finish_time - self.start_time
            print(elapsed_time.strftime("%H:%M:%S"))


        if os.path.exists(SF_filepath + ".dim"):
            print("Completed speckle-filtering:", SF_filepath)
        else:
            print("Completed speckle-filtering: data product saved to in-memory")

        self.intermediate_product = rd_corrected


    def createSourceImage(self, computedBand, realBand):
        """Work around to convert virtual band to real

        https://forum.step.esa.int/t/how-to-convert-virtual-bands-to-real-bands-using-snappy/3628/6
        """

        try:
            # assume computedBand is actually just a normal band
            return computedBand.getSourceImage()
        except RuntimeError as e:
            if e.message.startswith("java.lang.IllegalArgumentException"):
                # now assume computedBand is virtualBand
                virtualBand = snappy.jpy.cast(computedBand, snappy.VirtualBand)
                return snappy.VirtualBand.createSourceImage(realBand, virtualBand.getExpression())
            else:
                raise

    def convertComputedBandToBand(self, computedBand):
        """Work around to convert virtual band to real

        https://forum.step.esa.int/t/how-to-convert-virtual-bands-to-real-bands-using-snappy/3628/6
        """

        realBand = snappy.Band(
            computedBand.getName(),
            computedBand.getDataType(),
            computedBand.getRasterWidth(),
            computedBand.getRasterHeight()
        )

        realBand.setDescription(computedBand.getDescription())
        realBand.setValidPixelExpression(computedBand.getValidPixelExpression())
        realBand.setUnit(computedBand.getUnit())
        realBand.setSpectralWavelength(computedBand.getSpectralWavelength())
        realBand.setGeophysicalNoDataValue(computedBand.getGeophysicalNoDataValue())
        realBand.setNoDataValueUsed(computedBand.isNoDataValueUsed())

        if (computedBand.isStxSet()):
            realBand.setStx(computedBand.getStx())

        imageInfo = computedBand.getImageInfo()
        if imageInfo is not None:
            realBand.setImageInfo(imageInfo.clone())

        product = computedBand.getProduct()

        # "Check if all the frame with the raster data are close"
        # missing some stuff with topComponent

        bandGroup = product.getBandGroup()
        bandIndex = bandGroup.indexOf(computedBand)
        bandGroup.remove(computedBand)
        bandGroup.add(bandIndex, realBand)

        realBand.setSourceImage(createSourceImage(computedBand, realBand))


    def write_out_result(self, format='BEAM-DIMAP'):

        if format == 'BEAM-DIMAP':
            # Write data product with BEAM-DIM format to given file path
            # ProductIO.writeProduct(Product product, String filePath, String formatName)
            SF_filepath = Path(self.working_dir, self.name_with_safe + "_FINAL")
            print('WRITING OUT PRODUCT')

            ProductIO.writeProduct(self.intermediate_product, str(SF_filepath), 'BEAM-DIMAP')

            finish_time = datetime.datetime.now()# for calculation
            elapsed_time = finish_time - self.start_time
            print(elapsed_time.strftime("%H:%M:%S"))


        elif format == 'GEOTIFF':

            print('WRITING OUT TO GEOTIFF!!')

            sigma0_ortho_bands = ["Sigma0_VH_use_local_inci_angle_from_dem",
                                 "Sigma0_VV_use_local_inci_angle_from_dem"]

            print(self.intermediate_product)
            print(self.intermediate_product.getBandNames())
            for band in self.intermediate_product.getBandNames():
                print(band)

            self.convertComputedBandToBand(self.intermediate_product.getBand(sigma0_ortho_bands[0]))
            self.convertComputedBandToBand(self.intermediate_product.getBand(sigma0_ortho_bands[1]))

            sub_parameters = self.HashMap()
            sub_parameters.put('bandNames', ",".join(sigma0_ortho_bands)) # Should eventually look at using a local SRTM 1Sec DEM (instead of auto downloading)

            subset = GPF.createProduct("Subset", sub_parameters, self.intermediate_product)

            print(self.name_with_safe)

            final_output_name = Path(self.working_dir, self.name_with_safe + "_".join(self.operations_list))
            print('writing out final result')

            # Get a progressMonitor object
            monitor = self.createProgressMonitor()

            print('WRITING OUT PRODUCT')

            ProductIO.writeProduct(self.intermediate_product, str(final_output_name), 'GeoTIFF')

            finish_time = datetime.datetime.now() # for calculation
            elapsed_time = finish_time - self.start_time
            print(elapsed_time.strftime("%H:%M:%S"))


                # output to geotiff goes here

def createSourceImage(computedBand, realBand):
        """Work around to convert virtual band to real

        https://forum.step.esa.int/t/how-to-convert-virtual-bands-to-real-bands-using-snappy/3628/6
        """

        try:
            # assume computedBand is actually just a normal band
            return computedBand.getSourceImage()
        except RuntimeError as e:
            # if e.message.startswith("java.lang.IllegalArgumentException"):
            #     # now assume computedBand is virtualBand
            virtualBand = snappy.jpy.cast(computedBand, snappy.VirtualBand)
            return snappy.VirtualBand.createSourceImage(realBand, virtualBand.getExpression())
            # else:
            #     raise

def convertComputedBandToBand(computedBand):
    """Work around to convert virtual band to real

    https://forum.step.esa.int/t/how-to-convert-virtual-bands-to-real-bands-using-snappy/3628/6
    """

    realBand = snappy.Band(
        computedBand.getName(),
        computedBand.getDataType(),
        computedBand.getRasterWidth(),
        computedBand.getRasterHeight()
    )

    realBand.setDescription(computedBand.getDescription())
    realBand.setValidPixelExpression(computedBand.getValidPixelExpression())
    realBand.setUnit(computedBand.getUnit())
    realBand.setSpectralWavelength(computedBand.getSpectralWavelength())
    realBand.setGeophysicalNoDataValue(computedBand.getGeophysicalNoDataValue())
    realBand.setNoDataValueUsed(computedBand.isNoDataValueUsed())

    if (computedBand.isStxSet()):
        realBand.setStx(computedBand.getStx())

    imageInfo = computedBand.getImageInfo()
    if (imageInfo != None):
        realBand.setImageInfo(imageInfo.clone())

    product = computedBand.getProduct()

    # "Check if all the frame with the raster data are close"
    # missing some stuff with topComponent

    bandGroup = product.getBandGroup()
    bandIndex = bandGroup.indexOf(computedBand)
    bandGroup.remove(computedBand)
    bandGroup.add(bandIndex, realBand)

    realBand.setSourceImage(createSourceImage(computedBand, realBand))



def read_terrain_corrected_product(path_to_dim):
    print(path_to_dim)
    dataproduct_r = ProductIO.readProduct(path_to_dim)
    # will the computed band show up?
    for band in dataproduct_r.getBandNames():
        print(band)

    print('WRITING OUT TO GEOTIFF!!')

    sigma0_ortho_bands = ["Sigma0_VH_use_local_inci_angle_from_dem",
                            "Sigma0_VV_use_local_inci_angle_from_dem"]


    convertComputedBandToBand(dataproduct_r.getBand(sigma0_ortho_bands[0]))
    convertComputedBandToBand(dataproduct_r.getBand(sigma0_ortho_bands[1]))

    HashMap = jpy.get_type('java.util.HashMap')
    sub_parameters = HashMap()
    sub_parameters.put('bandNames', ",".join(sigma0_ortho_bands)) # Should eventually look at using a local SRTM 1Sec DEM (instead of auto downloading)

    subset = GPF.createProduct("Subset", sub_parameters, dataproduct_r)

    final_output_name = Path(Path(path_to_dim).parent, "test")

    print('writing out final result')

    # Get a progressMonitor object
    # monitor = self.createProgressMonitor()

    # print('WRITING OUT PRODUCT')

    ProductIO.writeProduct(subset, str(final_output_name), 'BEAM-DIMAP')



# # User parameters (Note: use uncompressed data and proper UNC paths)
# # SI & RS2 DUAL POL SAR data product contains 4 source bands representing 2 polarizations
# # 4 source bands: VH real amplitude, VV real amplitude, VH virtual intensity, VV virtual intensity
# working_directory = "K:\\AAFC\\Technical\\Data\\Raster\\Radar\\RS2\\Casselman\\Uncompressed\\"
# # DEM for SAR-Simulation
# externaldem = r"K:\AAFC\Technical\Data\Raster\DEM\Casselman\n45_w076_1arc_v3.tif"
# # Subset area for polygon vector
# wkt = "POLYGON((-75.387803 45.332917, -75.068535 45.332917, -75.068535 45.107280, -75.387803 45.332917))"

# # Initialize a SNAP's Graph Processing Framework instance, get registry and load operators in SPIs
# # The Graph Processing Framework makes extensive use of Java Advanced Imaging (JAI)
# # Configuring JAI TileCache and TileScheduler affects overall performance of GPF
# GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

# print("-------------")

# # Set object parameterisation dictionary
# HashMap = jpy.get_type('java.util.HashMap')

# # Set start time and loop counter
# start_timeflt = float(time.time()) # for calculation
# start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
# counter = 1
# print("Processing start time:", start_time)

# # Loop through folder with image data, extract file information and product data
# # Order of operation is changeable, just ensure to switch source and target data product inputs
# for folder in os.listdir(working_directory):
#     # Set S1 and RS2 file and metadata product paths
#     filepath = os.path.join(working_directory, folder)
#     preprocess_path = os.path.join(filepath, folder)
#     manifest_path = os.path.join(filepath, 'manifest.safe')
#     productxml_path = os.path.join(filepath, 'product.xml')

#     # Read S1 raw data product and assigns source bands for given file
#     # readProduct(File file, String... formatNames)
#     if folder.startswith("S1"):
#         dataproduct_r = snappy.ProductIO.readProduct(manifest_path)
#         srcbands = "Intensity_VV,Intensity_VH,Amplitude_VV,Amplitude_VH"
#         print("Reading S1 data product with polarization bands:", srcbands)
#     # Read RS2 raw data product and assigns source bands for given file
#     else:
#         dataproduct_r = snappy.ProductIO.readProduct(productxml_path)
#         srcbands = "Intensity_VV,Intensity_VH,Amplitude_VV,Amplitude_VH"
#         print("Reading RS2 data product with polarization bands:", srcbands)

#     print("-------------")

#     # Apply SPECKLE FILTER
#     print("Applying speckle-filter:", folder)

#     # Define object parameterisation for filtering
#     parameters = HashMap()
#     parameters.put('filter', "Gamma Map")
#     parameters.put('estimateENL', True)
#     parameters.put('filterSizeX', 7)
#     parameters.put('filterSizeY', 7)
#     parameters.put('numLooksStr', "1")
#     parameters.put('sourceBands', srcbands)
#     parameters.put('targetWindowStr', "3x3")
#     parameters.put('windowSize', "7x7")
#     print("Speckle filtering parameters:", parameters)

#     # Create speckle filter data product using 'Speckle-Filter' operator/parameters and raw data product as source product
#     # Speckle noise reduction can be applied either by spatial filtering or multilook processing
#     # Filtered product contains 4 real bands: 2 amplitude and intensity bands for each polarizations
#     # createProduct(String operatorName, Map<String,Object> parameters, Map<String,Product> sourceProducts)
#     SF_dataproduct = GPF.createProduct("Speckle-Filter", parameters, dataproduct_r)

#     # Write data product with BEAM-DIM format to given file path
#     # writeProduct(Product product, String filePath, String formatName)
#     #   SF_filepath = preprocess_path + "_SF"
#     #   ProductIO.writeProduct(SF_dataproduct, SF_filepath, 'BEAM-DIMAP')
#     # #   if os.path.exists(SF_filepath + ".dim"):
#     #     print("Completed speckle-filtering:", SF_filepath)
#     #   else:
#     # #     print("Completed speckle-filtering: data product saved to in-memory")

#     #   print("-------------")

#     #   # Apply SAR SIMULATION
#     #   print ("Applying SAR-Simulation:", SF_filepath)

#     # Re-define object parameters for SAR simulation
#     parameters = HashMap()
#     parameters.put('demName', "External DEM")
#     parameters.put('demResamplingMethod', "BICUBIC_INTERPOLATION")
#     parameters.put('externalDEMApplyEGM', True)
#     parameters.put('externalDEMNoDataValue', 0.0)
#     parameters.put('externalDEMFile', externaldem)
#     parameters.put('saveLayoverShadowMask', False)
#     parameters.put('sourceBands', srcbands) # by default all bands selected if not specified
#     print("SAR simulation parameters:", parameters)

#     # Create SAR simulation product using 'SAR-Simulation' operator/parameters and speckle filtered data product as source product
#     # Generates simulated SAR image using DEM, the Geocoding and orbit state vectors from a given SAR image,
#     # and mathematical modeling of SAR imaging geometry
#     # Simulated product contains an additional simulated_intensity band for a total of 5 bands
#     # createProduct(String operatorName, Map<String,Object> parameters, Map<String,Product> sourceProducts)
#     SS_dataproduct = GPF.createProduct("SAR-Simulation", parameters, SF_dataproduct)

#     # Write product with BEAM-DIM format to given file
#     # writeProduct(Product product, File file, String formatName)
#     SS_filepath = SF_filepath + "_SS"
#     ProductIO.writeProduct(SS_dataproduct, SS_filepath, 'BEAM-DIMAP')
#     if os.path.exists(SF_filepath + ".dim"):
#         print("Completed SAR-simulation:", SS_filepath)
#     else:
#         print("Completed SAR-simulation: data product saved to in-memory")

#     print("-------------")

#     # # Apply CROSS CORRELATION
#     # print ("Applying cross correlation:", SS_filepath)

#     # # Re-define object parameters for cross correlation
#     # parameters = HashMap()
#     # parameters.put('applyFineRegistration', True)
#     # parameters.put('courseRegistrationWindowHeight', "128")
#     # parameters.put('courseRegistrationWindowWidth', "128")
#     # parameters.put('coherenceThreshold', 0.6)
#     # parameters.put('coherenceWindowSize', 3)
#     # parameters.put('columnInterpFactor', "2")
#     # parameters.put('computeOffset', False)
#     # parameters.put('fineRegistrationOversampling', "16")
#     # parameters.put('fineRegistrationWindowAccAzimuth', "16")
#     # parameters.put('fineRegistrationWindowAccRange', "16")
#     # parameters.put('fineRegistrationWindowHeight', "32")
#     # parameters.put('fineRegistrationWindowWidth', "32")
#     # parameters.put('gcpTolerance', 0.5)
#     # parameters.put('inSAROptimized', False)
#     # parameters.put('maxIteration', 10)
#     # parameters.put('numGCPtoGenerate', 2000)
#     # parameters.put('onlyGCPsOnLand', True)
#     # parameters.put('rowInterpFactor', "2")
#     # parameters.put('useSlidingWindow', False)
#     # print("Cross-correlation parameters:", parameters)

#     # Create cross correlation product using cross Correlation operator/parameters and SAR simulated product as source product
#     # Cross correlation is a component of coregistration. It aligns one or more slave images
#     # with a master image in such a way that the each pixel from the co-registered slave image
#     # represents the same point on the Earth surface as its corresponding pixel in the master image.
#     # CC_filepath = SS_filepath + "_CC"
#     # CC_dataproduct = GPF.createProduct("Cross-Correlation", parameters, SS_dataproduct)
#     # if os.path.exists(CC_filepath + ".dim"):
#     #     print("Completed cross-Correlation:", CC_filepath)
#     # else:
#     #     print("Completed cross-Correlation: data product saved to in-memory")

#     # print("-------------")

#     # Apply SAR-SIM TERRAIN CORRECTION
#     print("Applying SARSim-Terrain-Correction:", CC_filepath)

#     # Re-define object parameters for terrain correction
#     parameters = HashMap()
#     parameters.put('alignToStandardGrid', False)
#     parameters.put('applyRadiometricNormalization', True)
#     parameters.put('auxFile', "Latest Auxiliary File")
#     parameters.put('imgResamplingMethod', "BILINEAR_INTERPOLATION")
#     parameters.put('incidenceAngleForSigma0', "Use local incidence angle from DEM")
#     parameters.put('mapProjection', "AUTO:42001")
#     parameters.put('openResidualsFile', True)
#     parameters.put('openShiftsFile', False)
#     parameters.put('outputComplex', False)
#     parameters.put('pixelSpacingInDegree', 0.0)
#     parameters.put('pixelSpacingInMeter', 0.0)
#     #parameters.put('rmsThreshold', 1.0)
#     parameters.put('saveBetaNought', False)
#     parameters.put('saveDEM', False)
#     parameters.put('saveGammaNought', False)
#     parameters.put('saveLatLong', False)
#     parameters.put('saveLocalIncidenceAngle', True)
#     parameters.put('saveProjectedLocalIncidenceAngle', True)
#     parameters.put('saveSelectedSourceBand', False)
#     parameters.put('saveSigmaNought', True)
#     parameters.put('standardGridOriginX', 0.0)
#     parameters.put('standardGridOriginY', 0.0)
#     parameters.put('warpPolynomialOrder', 1)
#     print("Terrain correction parameters:", parameters)

#     # Create terrain corrected product using terrain correction operator/parameters and cross correlation data product as source product
#     # Terrain-corrected product contains 6 bands: 2 calibrated sigma bands for each polarization,
#     # local incidence angle and projected local incidence angle bands, and 2 virtual calibrated bands corrected for local incidence angle
#     # Warp not applied if insufficient valid GCPs available and produces empty bands
#     TC_filepath = CC_filepath + "_TC"
#     TC_dataproduct = GPF.createProduct("SARSim-Terrain-Correction", parameters, CC_dataproduct)
#     # ProductIO.writeProduct(TC_dataproduct, TC_filepath, 'BEAM-DIMAP')
#     # if os.path.exists(TC_filepath + ".dim"):
#     #     print("Completed SARSim-Terrain-Correction:", TC_filepath)
#     # else:
#     #     print("Completed SARSim-Terrain-Correction: data product saved to in-memory")

#     # print("-------------")

#     # Apply SUBSET
#     print("Applying subset:", TC_filepath)

#     # Read geometry subset area polygon
#     WKTReader = jpy.get_type('com.vividsolutions.jts.io.WKTReader')
#     geom = WKTReader().read(wkt)

#     # Re-define object parameters for subsetting
#     parameters = HashMap()
#     parameters.put('copyMetadata', True)
#     parameters.put('sourceBands', "Sigma0_VV_use_local_inci_angle_from_dem,Sigma0_VH_use_local_inci_angle_from_dem")
#     parameters.put('geoRegion', geom)
#     print("Subsetting parameters:", parameters)

#     subset_filepath = TC_filepath + "_subset"
#     subset_dataproduct = GPF.createProduct("Subset", parameters, TC_dataproduct)
#     ProductIO.writeProduct(subset_dataproduct, subset_filepath, 'BEAM-DIMAP')
#     if os.path.exists(subset_filepath + ".dim"):
#         print("Completed subsetting:", subset_filepath)
#     else:
#         print("Completed subsetting: data product saved to in-memory")

#     print("-------------")

#     # Track time to process individual images
#     end_loopflt = float(time.time()) # for calculation
#     loop_time = end_loopflt - start_timeflt
#     if loop_time >= 3600:
#         print("Elapsed time for processing image #", counter, ":", loop_time/3600, "hr")
#     if loop_time >= 60 and loop_time <3600:
#         print("Image number", counter, "elapsed time:", loop_time/60, "min")
#     if loop_time >= 0 and loop_time <60:
#         print("Image number", counter, "elapsed time:", loop_time, "sec")

#     print("-------------")

#     # Increase counter
#     counter += 1

#     # Track time for entire processing
#     end_timeflt = float(time.time()) # for calculation
#     total_time = end_timeflt - start_timeflt
#     if total_time >= 3600:
#     print("Total time elapsed:", total_time/3600, "hr")
#     if total_time >= 60 and total_time <3600:
#     print("Total time elapsed:", total_time/60, "min")
#     if total_time >= 0 and total_time <60:
#     print("Total time elapsed:", total_time, "sec")

