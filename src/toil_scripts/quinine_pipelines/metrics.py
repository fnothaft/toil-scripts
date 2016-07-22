#!/usr/bin/env python2.7

import argparse
import os

from toil.job import Job

from toil_scripts.tools.qc import ( call_quinine_af_native,
                                    call_quinine_contamination_native,
                                    call_quinine_hs_native,
                                    call_quinine_rna_native )                                    
from toil_scripts.quinine_pipelines.adam_helpers import ( bam_to_adam_native,
                                                          feature_to_adam_native,
                                                          vcf_to_adam_native )

def run_rna_qc(job,
               reads, transcriptome, output_path,
               memory,
               adam_native_path, quinine_native_path):
    '''
    Runs a job that computes various RNA-seq quality control statistics.
    
    :param toil.job.Job job: The toil job running this function.
    :param str reads: Path to the input reads in SAM/BAM format.
    :param str transcriptome: Path to the transcriptome definition (GTF/GFF).
    :param str output_path: Path to write the statistics at.
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
    feature_to_adam_native(transcriptome, adam_features, memory, adam_native_path)

    # run the qc job
    call_quinine_rna_native(adam_reads, adam_features, output_path,
                            local_dir,
                            memory, quinine_native_path)
    

def run_targeted_qc(job,
                    reads, bait, targets, output_path,
                    memory,
                    adam_native_path, quinine_native_path):
    '''
    Runs a job that computes various quality control statistics for reads
    sequenced using a hybrid-selection panel that requires targeting.
    
    :param toil.job.Job job: The toil job running this function.
    :param str reads: Path to the input reads in SAM/BAM format.
    :param str bait: Path to the description of the baited regions.
    :param str targets: Path to the description of the regions targeted.
    :param str output_path: Path to write the statistics at.
    :param int memory: GB of memory to allocate.
    :param str adam_native_path: The path where ADAM is installed.
    :param str quinine_native_path: The path where Quinine is installed.
    '''

    # get a temp work directory
    local_dir = job.fileStore.getLocalTempDir()

    # convert the reads to ADAM format
    adam_reads = os.path.join(local_dir, 'reads.adam')
    bam_to_adam_native(reads, adam_reads, memory, adam_native_path)

    # convert the bait features to ADAM format
    adam_bait = os.path.join(local_dir, 'bait.adam')
    feature_to_adam_native(bait, adam_bait, memory, adam_native_path)

    # convert the target features to ADAM format
    adam_targets = os.path.join(local_dir, 'targets.adam')
    feature_to_adam_native(targets, adam_targets, memory, adam_native_path)

    # run the metrics
    call_quinine_hs_native(adam_reads, adam_targets, adam_bait, output_path,
                           local_dir,
                           memory, quinine_native_path)


def run_contamination_estimation(job,
                                 reads, population, sample_vcf, output_path,
                                 memory,
                                 adam_native_path, quinine_native_path):
    '''
    Runs a job that computes various quality control statistics for reads
    sequenced using a hybrid-selection panel that requires targeting.
    
    :param toil.job.Job job: The toil job running this function.
    :param str reads: Path to the input reads in SAM/BAM format.
    :param str bait: Path to the description of the baited regions.
    :param str targets: Path to the description of the regions targeted.
    :param str output_path: Path to write the statistics at.
    :param int memory: GB of memory to allocate.
    :param str adam_native_path: The path where ADAM is installed.
    :param str quinine_native_path: The path where Quinine is installed.
    '''

    # get a temp work directory
    local_dir = job.fileStore.getLocalTempDir()

    # convert the reads to ADAM format
    adam_reads = os.path.join(local_dir, 'reads.adam')
    bam_to_adam_native(reads, adam_reads, memory, adam_native_path)

    # convert the sample vcf to ADAM format
    adam_calls = os.path.join(local_dir, 'sample.adam')
    vcf_to_adam_native(sample_vcf, adam_calls, memory, adam_native_path)

    # compute MAF's
    maf_annotations = os.path.join(local_dir, 'mafs.adam')
    call_quinine_af_native(population, maf_annotations,
                           local_dir,
                           memory,
                           quinine_native_path)

    # estimate contamination
    call_quinine_contamination_native(adam_reads,
                                      adam_calls,
                                      maf_annotations,
                                      output_path,
                                      local_dir,
                                      memory,
                                      quinine_native_path)


def __add_common_args(parser):
    '''
    Adds commonly used arguments to a subparser.

    :param argparse.Subparser parser: A subparser to add arguments to.
    '''

    parser.add_argument('--output',
                        help='Location to write outputs to.',
                        required=True)
    parser.add_argument('--memory',
                        help='The amount of memory to allocate, in GB. Defaults to 1.',
                        type=int,
                        default=1)
    parser.add_argument('--adam_native_path',
                        help='The native path where ADAM is installed.'
                        'Defaults to /opt/cgl-docker-lib/adam',
                        default='/opt/cgl-docker-lib/adam',
                        type=str)
    parser.add_argument('--quinine_native_path',
                        help='The native path where Quinine is installed.'
                        'Defaults to /opt/cgl-docker-lib/quinine',
                        default='/opt/cgl-docker-lib/quinine',
                        type=str)
    Job.Runner.addToilOptions(parser)


def main():
    '''
    Parses arguments and starts the job.
    '''

    # build the argument parser
    parser = argparse.ArgumentParser()

    # we run three different commands: hs, cont, rna
    subparsers = parser.add_subparsers(dest='command')
    parser_rna = subparsers.add_parser('rna', help='Runs the RNA QC metrics.')
    parser_hs = subparsers.add_parser('targeted',
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
    __add_common_args(parser_rna)

    # add arguments to the hs panel
    parser_hs.add_argument('--reads',
                           help='The aligned reads.',
                           type=str,
                           required=True)
    parser_hs.add_argument('--bait',
                           help='The bait used for capturing this panel.',
                           type=str,
                           required=True)
    parser_hs.add_argument('--targets',
                           help='The regions covered by this panel.',
                           type=str,
                           required=True)
    __add_common_args(parser_hs)

    # add arguments for contaimination estimation
    parser_cont.add_argument('--reads',
                             help='The aligned reads.',
                             type=str,
                             required=True)
    parser_cont.add_argument('--population',
                             help='The VCF to derive allele frequencies from.',
                             type=str,
                             required=True)
    parser_cont.add_argument('--sample-vcf',
                             help='A VCF containing known genotypes for the sample.',
                             type=str,
                             required=True)
    __add_common_args(parser_cont)    
    
    # parse the arguments
    args = parser.parse_args()

    # check which command got called, and set up and run
    if args.command == 'rna':
        Job.Runner.startToil(Job.wrapJobFn(run_rna_qc,
                                           args.reads,
                                           args.transcriptome,
                                           args.output,
                                           args.memory,
                                           args.adam_native_path,
                                           args.quinine_native_path), args)
    elif args.command == 'targeted':
        Job.Runner.startToil(Job.wrapJobFn(run_targeted_qc,
                                           args.reads,
                                           args.bait,
                                           args.targets,
                                           args.output,
                                           args.memory,
                                           args.adam_native_path,
                                           args.quinine_native_path), args)
    elif args.command == 'contamination':
        Job.Runner.startToil(Job.wrapJobFn(run_contamination_estimation,
                                           args.reads,
                                           args.population,
                                           args.sample_vcf,
                                           args.output,
                                           args.memory,
                                           args.adam_native_path,
                                           args.quinine_native_path), args)
    else:
        raise ValueError('Unknown command: %s' % args.command)
    
if __name__ == '__main__':
    main()
