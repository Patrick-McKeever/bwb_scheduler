# Biodepot Scheduler

## Description
This scheduler is based on Temporal.io workflow framework with key modifications to optimize bioinformatics workflows. We support a hybrid architecture across cloud, HPC, and local servers. Asynchronous execution is also supported to reduce execution time. This workflow has been tested using bulk RNA-seq datasets.


## Installation

### Dependencies
- Singularity > 3.5.0
- Docker (if running locally)
- Singularity / Docker nvidia toolkits (if using GPU-based workflows)
- Rsync (if using HPC)

**TODO**: Make an install script. For now, you can just compile the code, but I'll get to this shortly.

## CLI

**Note**: All CLI commands expect `BWB_SCHED_DIR` to be set as an environment variable; this will be mounted to `/data/` locally. (If using a storage ID to isolate workflow FSs, then `BWB_SCHED_DIR/[storageID]` will be mounted instead). Files should be staged here before running.

### OWS to JSON conversion
```
Usage:
  bwbScheduler convertOWS [OWS_DIR_PATH] [flags]

Flags:
  -h, --help            help for convertOWS
  -o, --output string   File path where the converted workflow will be stored in JSON format.
```

### Dry Run
```
Usage:
  bwbScheduler dryRun [WORKFLOW_FILE] [flags]

Flags:
  -h, --help                help for dryRun
  -p, --param stringArray   String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is the title or numeric ID of a node defined in the workflow JSON, PARAM_NAME is one of its parameters, and value is a JSON-parsable string giving the desired value of this parameter. This will override the default parameters given in the JSON.
```

### Running a workflow
```
Usage:
  bwbScheduler run WORKFLOW_FILE [CONFIG_FILE] [flags]

Flags:
      --cpus int                Max number of CPU cores to use on local worker.
      --gpus int                Max number of GPUs to use on local worker.
  -h, --help                    help for run
      --noTemporal              Run workflow without temporal
  -p, --param stringArray       String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is the title or numeric ID of a node defined in the workflow JSON, PARAM_NAME is one of its parameters, and value is a JSON-parsable string giving the desired value of this parameter. This will override the default parameters given in the JSON.
      --ram bytes               Max amount of RAM to use for the local worker as a string formed like "10GB", "500MB", etc. Default is 70% of system RAM.
      --softFail                Continue running workflow after encountering individual job failures,until all cmds not dependant on the failed job have run.
      --storageId string        Storage ID of the workflow. Workflows run with the same storage ID (and the same master FS) will share filesystems.
      --temporalWfName string   Run ID for temporal workflow. Cannot be used in conjunction with --noTemporal.
      --workerName string       Temporal queue name of local worker. Cannot be used in conjunction with --noTemporal.
```

## Example usage
```
# Set BWB_SCHED_DIR and stage dependencies there.
export BWB_SCHED_DIR=/home/user/SCHED_STORAGE
mkdir -p /home/user/SCHED_STORAGE/storageID/fastqFiles
mv R1.fastq R2.fastq /home/user/SCHED_STORAGE/storageID/fastqFiles


# Run bulk RNA seq workflow on R1/R2.fastq using hybrid HPC config.
go run main.go run --storageId storageID -p Start.fastqDir=/data/fastqFiles test_workflows/bulkrna_async.json test_workflows/bulkrna_config_hybrid.json
```