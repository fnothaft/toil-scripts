from subprocess import check_call

from toil_scripts.adam_pipeline.adam_preprocessing import call_adam

def __call_adam_native(cmd, memory, native_path):
    '''
    Calls ADAM running in Spark local mode, where ADAM is not in a docker container.

    :param list<str> cmd: ADAM command line arguments
    :param int memory: Amount of memory in GB to allocate.
    :param str native_path: String path to the ADAM install directory.
    '''

    check_call(['%s/bin/adam-submit' % native_path,
                '--master', 'local[*]',
                '--conf', 'spark.driver.memory=%dg' % memory,
                '--'] + cmd)


def bam_to_adam_native(bam, parquet, memory, native_path):
    '''
    Converts a BAM file into an ADAM AlignmentRecord Parquet file.

    :param str bam: Path to input SAM/BAM file.
    :param str parquet: Path to save Parquet file at.
    :param int memory: Amount of memory in GB to allocate.
    :param str native_path: String path to the ADAM install directory.
    '''

    __call_adam_native(['transform', bam, parquet], memory, native_path)


def feature_to_adam_native(feature, parquet, memory, native_path):
    '''
    Converts a feature file (e.g., BED, GTF, GFF) into an ADAM Feature Parquet
    file.

    :param str feature: Path to input BED/GTF/GFF/NarrowPeak file.
    :param str parquet: Path to save Parquet file at.
    :param int memory: Amount of memory in GB to allocate.
    :param str native_path: String path to the ADAM install directory.
    '''
    
    __call_adam_native(['features2adam', feature, parquet], memory, native_path)


def vcf_to_adam_native(vcf, parquet, memory, native_path):
    '''
    Converts a VCF file into an ADAM Genotype Parquet file.

    :param str bam: Path to input VCF file.
    :param str parquet: Path to save Parquet file at.
    :param int memory: Amount of memory in GB to allocate.
    :param str native_path: String path to the ADAM install directory.
    '''

    __call_adam_native(['vcf2adam', vcf, parquet], memory, native_path)
