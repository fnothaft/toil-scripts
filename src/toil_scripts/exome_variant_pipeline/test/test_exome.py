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
import shutil
import subprocess
import textwrap

from boto.s3.connection import S3Connection, Bucket, Key

from toil_scripts.lib import get_work_directory


def test_exome(tmpdir):
    workdir = get_work_directory()
    create_config_and_manifest(workdir)
    # Call Pipeline
    try:
        base_command = ['toil-exome', 'run',
                        '--config', os.path.join(workdir, 'config-toil-exome.yaml'),
                        os.path.join(workdir, 'jstore'),
                        '--workDir', workdir]
        # Run with manifest
        subprocess.check_call(base_command + ['--manifest', os.path.join(workdir, 'manifest-toil-exome.tsv')])
    finally:
        shutil.rmtree(workdir)
        conn = S3Connection()
        b = Bucket(conn, 'cgl-driver-projects')
        k = Key(b)
        k.key = 'test/ci/exome-ci-test.tar.gz'
        k.delete()


def create_config_and_manifest(path):
    """Creates config file for test at path"""
    config_path = os.path.join(path, 'config-toil-exome.yaml')
    manifest_path = os.path.join(path, 'manifest-toil-exome.tsv')
    with open(config_path, 'w') as f:
        f.write(generate_config())
    with open(manifest_path, 'w') as f:
        f.write(generate_manifest())


def generate_config():
    return textwrap.dedent("""
    reference: s3://cgl-pipeline-inputs/exome/ci/chr6.hg19.fa
    phase: s3://cgl-pipeline-inputs/exome/ci/chr6.phase.hg19.vcf
    mills: s3://cgl-pipeline-inputs/exome/ci/chr6.Mills.indels.hg19.sites.vcf
    dbsnp: s3://cgl-pipeline-inputs/exome/ci/chr6.dbsnp.hg19.vcf
    cosmic: s3://cgl-pipeline-inputs/exome/ci/chr6.cosmic.hg19.vcf
    run-mutect: true
    run-pindel: true
    run-muse: true
    preprocessing: true
    output-dir:
    s3-dir: s3://cgl-driver-projects/test/ci
    ssec:
    gtkey:
    ci-test: true
    """[1:])


def generate_manifest():
    return textwrap.dedent("""
        exome-ci-test\ts3://cgl-pipeline-inputs/exome/ci/chr6.normal.bam\ts3://cgl-pipeline-inputs/exome/ci/chr6.tumor.bam
        """[1:])
