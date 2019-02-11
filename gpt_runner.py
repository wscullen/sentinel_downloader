"""Runs a subprocess and calls the bash script with the appropriate command line
arguments.

graph processsing toolkit can be run from the command line using GPT

using the setup bash and bat scripts, this python script will:

# Take appropriate args (product location, window size, target location)
# Generate .properties file
# Run a subporcess that calls the processDataset(.bash|.bat) with the positional args it requires
# Report the success or failure of the process based on the stdout and stderr of the bash script

"""

from pathlib import Path
import subprocess

import asyncio


class GPTRunner():
    def __init__(self, product_path, target_path, graph_xml_file, arg_dict, process_id):

        self.product_path = product_path
        self.product_name = Path(self.product_path).name.split('.')[0]

        self.graph_xml_file = graph_xml_file

        self.target_path = target_path
        self.arg_dict = arg_dict
        self.process_id = process_id


    def generate_properties_file(self):
        """Based on the arg dict, create a properties file that GPT tool can read

            sourceProductManifestFile = absolute path to Manifest.safe file in product dir

            filterSize = x and y size of Gamma Map filter 7, 9 or 11 are good choices
            windowSize = f'{filterSize}x{filterSize} string representing window size based on filter
            bitDepth = target bitdepth of bands (float32 or int16)
            outputTargetFilePath = abs file path to target file name (no suffix)
        """

        source_product_manifest_file_str = 'sourceProductManifestFile'
        source_product_manifest_file_val = str(Path(self.product_path))

        filter_size_str = 'filterSize'
        filter_size_val = str(self.arg_dict['filterSize'])

        window_size_str = 'windowSize'
        window_size_val = f"{self.arg_dict['filterSize']}x{self.arg_dict['filterSize']}"

        bit_depth_str = 'bitDepth'
        bit_depth_val = self.arg_dict['bitDepth']

        output_target_file_path_str = 'outputTargetFilePath'

        target_suffix = '_' + Path(self.graph_xml_file).name.split('.')[0] + '.tif'
        target_file_name = self.product_name + target_suffix

        output_target_file_path_val = str(Path(self.target_path, target_file_name))

        properties_dict = {}

        with open(f'process{self.process_id}.properties', 'w') as f:
            f.write(f"{source_product_manifest_file_str}={source_product_manifest_file_val}\n")
            properties_dict[source_product_manifest_file_str] = source_product_manifest_file_val
            f.write(f"{filter_size_str}={filter_size_val}\n")
            properties_dict[filter_size_str] = filter_size_val
            f.write(f"{window_size_str}={window_size_val}\n")
            properties_dict[window_size_str] = window_size_val
            f.write(f"{bit_depth_str}={bit_depth_val}\n")
            properties_dict[bit_depth_str] = bit_depth_val
            f.write(f"{output_target_file_path_str}={output_target_file_path_val}\n")
            properties_dict[output_target_file_path_str] = output_target_file_path_val

        return properties_dict

    async def read_stream(self, stream, cb, result_string, ws):

        # logging.basicConfig(level=logging.DEBUG, format="%(asctime)s-%(thread)d-%(process)d-%(funcName)s-%(message)s")
        # logger.setFormatter(formatter)
        # ch.setFormatter(formatter)

        while True:
            line = await stream.readline()
            if line:
                cb(line)  # just print the line in a simple lambda
                # await logger.info(line.decode('utf-8').strip())

                result_string.append(line.decode('utf-8').strip())
            else:
                break


    async def run(self, cmd):

        print(cmd)

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)

        result_string = []
        result_err_string = []

        final_result = False

        await asyncio.wait([
                            self.read_stream(proc.stdout, lambda x: x, result_string, None),
                            self.read_stream(proc.stderr, lambda x: print(f"STDERR: {x.decode('utf-8').strip()}"), result_err_string, None)
                        ])

        print(f'[{cmd!r} exited with {proc.returncode}]')


        if len(result_string) > 0:
            # print(f'[stdout]\n{stdout.decode()}')
            print(result_string)
        if len(result_err_string) > 0:
            # print(f'[stderr]\n{stderr.decode()}')
            print(result_err_string)

        if proc.returncode != 0:
            return False
        else:
            return True

    def run_graph(self):
        gpt_path = Path('/home/cullens/Development/sentinel_downloader/gpt_graphs')
        properties_path = Path('/home/cullens/Development/sentinel_downloader/', f'process{self.process_id}.properties')

        self.generate_properties_file()

        bash_script_path = Path(gpt_path, 'processDataset.bash')
        graph_xml_path = Path(gpt_path, 's1', 'ao_co_sf_tc_flt32_all.xml')

        result = asyncio.run(self.run(
            f'{bash_script_path} {self.graph_xml_file} {properties_path}'.split(' ')
        ))

        return result

if __name__ == "__main__":
    pass
    # product_path_arg = '/home/cullens/Development/s2d2/temp/S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB.SAFE'
    # target_path_arg = '/home/cullens/Development/s2d2/temp'
    # path_to_graph_xml = '/home/cullens/Development/sentinel_downloader/gpt_graphs/s1/ao_co_sf_tc_flt32_all.xml'

    # arg_dict = {
    #         'filterSize': 7,
    #         'bitDepth': 'float32'
    # }

    # runner = gpt_runner.GPTRunner(product_path_arg,
    #                               target_path_arg,
    #                               path_to_graph_xml,
    #                               arg_dict,
    #                               1)
