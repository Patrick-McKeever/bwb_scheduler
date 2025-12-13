import os
import argparse
import json
import sys

"""
This script sets up work directories and expected output files for the star_salmon_dashboard workflow that are not already specified in the widget's input entries.

"""

def list_to_array(lis):
    """
    Converts a python string list into a string bash format.
    `"<str1>" "<str2>" "<str3>"`

    lis: Python list to convert to bash array string format
    """
    stringbuilt = ''
    for item in lis[:-1]:
        stringbuilt += '\"'
        stringbuilt += item
        stringbuilt += '\" '
    stringbuilt += '\"'
    stringbuilt += lis[-1]
    stringbuilt += '\"'

    return stringbuilt


def string_output(var, output):
    """
    Transfers a variable and its content (as a string) to an output widget.
    Use a printf statement to pipe into /tmp/output/<output> to properly load content into output.

    var: Python variable to transfer, either string or list
    output: Output parameter name specified in the original widget (in "Outputs" tab)
    """
    
    # Variable is a list, then need to convert it to bash list string
    if type(var) == list:
        var = list_to_array(var)
    

    # Load variables as outputs (output name found in Outputs tab)
    os.system('printf "{}"  > "/tmp/output/{}"'.format(var, output))

# def pattern_string_output(var, output):
# {'root': '/data/fastqdir', 'pattern': '**/*.fastq', 'findFile': True, 'findDir': False, 'value': ['/data/fastqdir/testfile.fastq']}


def load_dashboard_groups(json_file_path):
    """
    Reads a dashboard JSON file and extracts group names, sample SRR IDs, and layouts.

    Returns:
        group_names: List of group names
        group_samples: List of lists, each containing dicts with 'srr_id' and 'layout'
    """
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    group_names = []
    group_samples = []

    for group in data.get("groups", []):
        group_names.append(group.get("name", ""))
        samples = []
        for sample in group.get("samples", []):
            samples.append({
                "srr_id": sample.get("srr_id", ""),
                "layout": sample.get("layout", "")
            })
        group_samples.append(samples)

    return group_names, group_samples

# Example usage:
# group_names, group_samples = load_dashboard_groups("sample_dashboard_data_v2.json")
# print(group_names)
# print(group_samples)


def main():
    parser = argparse.ArgumentParser(
        description="Converted Bash script to Python using argparse"
    )
    # parser.add_argument('--fastqfiles',      required=True, help="Original fastqfiles string")
    # parser.add_argument('--s3downloaddir',   required=True, help="Original S3 download dir")
    # parser.add_argument('--trimmeddir',      required=True, help="Trimmed-files directory")
    parser.add_argument('--work_dir',        required=True, help="Work directory")
    parser.add_argument('--genome_dir',      required=True, help="Genome directory")
    parser.add_argument('--download_dir',    required=True, help="Download directory")
    parser.add_argument('--json_file',      required=True, help="Path to JSON file containing group data")
    args = parser.parse_args()

    # # -- trimmedfastqfiles logic:
    # tf = args.fastqfiles.replace(f'"{args.s3downloaddir}', f'"{args.trimmeddir}')
    # tf = tf.replace('R1_001.fastq', 'R1_001_val_1.fq')
    # tf = tf.replace('R2_001.fastq', 'R2_001_val_2.fq')
    # print(tf)
    # string_output(tf, 'trimmedfastqfiles')

    # # -- print trimmedfastqfiles:
    # print("trimmedfastqfiles:", tf)

    # -- mkdir -p equivalents:
    for d in (args.work_dir, args.genome_dir, args.download_dir):
        try:
            os.makedirs(d, exist_ok=True)
            print(f"Directory {d} created successfully.")
        except Exception as e:
            print(f"Failed to create {d}: {e}", file=sys.stderr)
            sys.exit(1)
    
    # Read JSON file
    group_names, group_samples  = load_dashboard_groups(args.json_file)
    
    # Grab all SRR IDs and layouts from the groups
    srrids_all = []
    for group in group_samples:
        for sample in group:
            srr_id = sample['srr_id']
            srrids_all.append(srr_id)

    string_output(srrids_all, 'srridsAll')
    # string_output(normal_ids, 'srridsNormal')
    # string_output(disease_ids, 'srridsDisease')

    # # -- print srrids:
    # print("srridsNormal:", " ".join(normal_ids))
    # print("srridsDisease:", " ".join(disease_ids))

    # -- print final combined list:
    print("srridsAll:", " ".join(srrids_all))

    # If there exists at least one "PAIRED" layout, then set the paired flag to true
    paired = any(sample['layout'] == 'PAIRED' for group in group_samples for sample in group)

    if paired:
        string_output(1, 'paired')
        print("paired-end: True")
    else:
        string_output(0, 'paired')
        print("paired-end: False")

    # Make placeholder for Jupyter Notebook file output, stored in work directory
    notebook_output = args.work_dir + "/dashboard_differential_expression_analysis_output.ipynb"
    string_output(notebook_output, "notebookOutput")
 

    # TODO: Using the layouts, anticipate the expected output files (_1.fastq for single-end, _1.fastq and _2.fastq for paired-end)
    # For now, we will just output the group names and samples



if __name__ == '__main__':
    main()
