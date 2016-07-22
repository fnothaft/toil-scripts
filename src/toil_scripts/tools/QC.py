import os
from subprocess import check_call

from toil_scripts.lib import require
from toil_scripts.lib.files import tarball_files
from toil_scripts.lib.programs import docker_call


def run_fastqc(job, r1_id, r2_id):
    """
    Run Fastqc on the input reads

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq read 1
    :param str r2_id: FileStoreID of fastq read 2
    :return: FileStoreID of fastQC output (tarball)
    :rtype: str
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
    parameters = ['/data/R1.fastq']
    output_names = ['R1_fastqc.html']
    if r2_id:
        job.fileStore.readGlobalFile(r2_id, os.path.join(work_dir, 'R2.fastq'))
        parameters.extend(['-t', '2', '/data/R2.fastq'])
        output_names.append('R2_fastqc.html')
    docker_call(tool='quay.io/ucsc_cgl/fastqc:0.11.5--be13567d00cd4c586edf8ae47d991815c8c72a49',
                work_dir=work_dir, parameters=parameters)
    output_files = [os.path.join(work_dir, x) for x in output_names]
    tarball_files(tar_name='fastqc.tar.gz', file_paths=output_files, output_dir=work_dir)
    return job.fileStore.writeGlobalFile(os.path.join(work_dir, 'fastqc.tar.gz'))


def __call_quinine(arguments, memory,
                   run_local=False, local_dir=None,
                   native_path=None,
                   leader_ip=None):
    '''
    Run Quinine (https://github.com/bigdatagenomics/quinine).

    :param list<string> arguments: Arguments to pass to Quinine.
    :param int memory: Amount of memory in GiB to allocate per node.
    :param bool run_local: If true, runs quinine in local mode. Default is false.
    :param None or string local_dir: If provided, path to a local working
       directory. Must be provided if run_local is true.
    :param None or string native_path: If set, the path to quinine. If unset,
       runs quinine via Docker. Default is None (unset).
    :param None or SparkMasterAddress leader_ip: If provided, IP of the Spark
       leader node. Must be provided if run_local is false.
    '''

    # TODO: factor this out into a common lib
    SPARK_MASTER_PORT=7077
    HDFS_MASTER_PORT=8020
    
    # validate input arguments
    require((run_local and leader_ip is None) or
            (not run_local and leader_ip is not None),
            "Either run_local ({0}) must be set or leader_ip ({1}) must not be None.".format(run_local,
                                                                                             leader_ip))
    require(not run_local or local_dir is not None,
            "If run_local is set, local_dir must be set.")

    # are we running locally, or not? set up master configuration
    if inputs.run_local:
        master = ["--master", "local[*]"]
        work_dir = local_dir
    else:
        master = ["--master",
                  ("spark://%s:%s" % (master_ip, SPARK_MASTER_PORT)),
                  "--conf", ("spark.hadoop.fs.default.name=hdfs://%s:%s" % (master_ip, HDFS_MASTER_PORT))]
        work_dir = '.'

    # set default parameters
    default_params = master + [
                      "--conf", ("spark.driver.memory=%sg" % inputs.memory),
                      "--conf", ("spark.executor.memory=%sg" % inputs.memory),
                      "--conf", "spark.driver.maxResultSize=0",
                      # set max result size to unlimited, see #177
                      "--"]

    # are we running via docker or natively?
    if native_path is None:
        docker_call(rm=False,
                    tool="quay.io/ucsc_cgl/quinine",
                    docker_parameters=master_ip.docker_parameters(['--net=host']),
                    parameters=(default_params + arguments),
                    work_dir=work_dir,
                    mock=False)
    else:
        check_call(["%s/bin/quinine-submit" % native_path] +
                   default_params +
                   arguments)


def call_quinine_rna_native(reads,
                            transcriptome,
                            work_dir,
                            native_path):
    '''
    Runs quinine to compute RNA-seq QC stats.

    :param str reads: Local path to input reads.
    :param str transcriptome: Local path to transcriptome definition.
    :param str work_dir: Local path to temp working directory.
    :param str native_path: Local path to quinine.
    '''

    __call_quinine(['rnaMetrics',
                    reads, transcriptome],
                   run_local=True, local_dir=work_dir
                   native_path=native_path)


def call_quinine_hs_native(reads,
                           panel,
                           bait,
                           work_dir,
                           native_path):
    '''
    Runs quinine to compute stats for a hybrid-selection targeted sequencing panel.

    :param str reads: Local path to input reads.
    :param str panel: Local path to definition of regions targeted by panel.
    :param str bait: Local path to definition of regions tiled by bait.
    :param str work_dir: Local path to temp working directory.
    :param str native_path: Local path to quinine.
    '''

    __call_quinine(['panelMetrics',
                    reads, panel, bait,
                    ],
                   run_local=True, local_dir=work_dir
                   native_path=native_path)


def call_quinine_contamination_native(reads,
                                      genotypes,
                                      annotations,
                                      work_dir,
                                      native_path):
    '''
    Runs quinine to estimate inter-sample contamination.

    :param str reads: Local path to input reads.
    :param str genotypes: Local path to genotypes.
    :param str annotations: Local path to annotations.
    :param str work_dir: Local path to temp working directory.
    :param str native_path: Local path to quinine.
    '''
    
    __call_quinine(['estimateContamination',
                    reads, genotypes, annotations],
                   run_local=True, local_dir=work_dir,
                   native_path=native_path)
