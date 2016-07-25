#!/usr/bin/env python2.7

import argparse
import os

from toil.job import Job

from toil_scripts.tools.qc import call_quinine_rna_native
from toil_scripts.quinine_pipelines.adam_helpers import ( bam_to_adam_native,
                                                          features_to_adam_native )

def run_rna_qc(job,
               reads, transcriptome,
               memory,
               adam_native_path, quinine_native_path):
    '''
    Runs a job that computes various RNA-seq quality control statistics.
    
    :param toil.job.Job job: The toil job running this function.
    :param str reads: Path to the input reads in SAM/BAM format.
    :param str transcriptome: Path to the transcriptome definition (GTF/GFF).
    :param int memory: GB of memory to allocate.
    :param str adam_native_path: The path where ADAM is installed.
    :param str quinine_native_path: The path where Quinine is installed.
    '''

    # get a temp work directory
    local_dir = job.fileStore.getLocalTempDir()

    # convert the reads to ADAM format
    adam_reads = os.path.join(local_dir, 'reads.adam')
    bam_to_adam_native(reads, adam_reads, memory, adam_native_path)

    # convert the features to ADAM format
    adam_features = os.path.join(local_dir, 'transcriptome.adam')
    features_to_adam_native(transcriptome, adam_features, memory, adam_native_path)

    # run the qc job
    call_qunine_rna_native(adam_reads, adam_features, local_dir, memory, quinine_native_path)
    

def main():
    '''
    Parses arguments and starts the job.
    '''

    # build the argument parser
    parser = argparse.ArgumentParser()

    # we run three different commands: hs, cont, rna
    subparsers = parser.add_subparsers(dest='command')
    parser_rna = subparsers.add_parser('rna', help='Runs the RNA QC metrics.')
    parser_hs = subparsers.add_parser('hs',
                                      help='Runs the QC metrics for a targeted sequencing protocol.')
    parser_cont = subparsers.add_parser('contamination',
                                        help='Runs the contamination estimator.')

    # add arguments to the rna panel
    parser_rna.add_argument('--reads',
                            help='The RNA-seq reads.',
                            type=str,
                            required=True)
    parser_rna.add_argument('--transcriptome',
                            help='The transcriptome description (e.g., a GENCODE GTF)',
                            type=str,
                            required=True)
    parser_rna.add_argument('--memory',
                            help='The amount of memory to allocate, in GB. Defaults to 1.',
                            type=int,
                            default=1)
    parser_rna.add_argument('--adam_native_path',
                            help='The native path where ADAM is installed.'
                            'Defaults to /opt/cgl-docker-lib/adam',
                            default='/opt/cgl-docker-lib/adam',
                            type=str)
    parser_rna.add_argument('--quinine_native_path',
                            help='The native path where Quinine is installed.'
                            'Defaults to /opt/cgl-docker-lib/quinine',
                            default='/opt/cgl-docker-lib/quinine',
                            type=str)
    Job.Runner.addToilOptions(parser_rna)
    
    # parse the arguments
    args = parser.parse_args()

    # check which command got called, and set up and run
    if args.command == 'rna':
        Job.Runner.startToil(Job.wrapJobFn(run_rna_qc,
                                           args.reads,
                                           args.transcriptome,
                                           args.memory,
                                           args.adam_native_path,
                                           args.quinine_native_path))
    else:
        raise NotImplementedError()
    
if __name__ == '__main__':
    main()
