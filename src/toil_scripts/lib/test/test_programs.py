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


def test_docker_call(tmpdir):
    from toil_scripts.lib.programs import docker_call
    work_dir = str(tmpdir)
    parameter = ['--help']
    tool = 'quay.io/ucsc_cgl/samtools'
    docker_call(work_dir=work_dir, parameters=parameter, tool=tool)
    # Test outfile
    fpath = os.path.join(work_dir, 'test')
    with open(fpath, 'w') as f:
        docker_call(tool='ubuntu', env=dict(foo='bar'), parameters=['printenv', 'foo'], outfile=f)
    assert open(fpath).read() == 'bar\n'
