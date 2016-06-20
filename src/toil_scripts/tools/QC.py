#
# Copyright 2015-2016, Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os

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
