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
from toil.job import Job
from toil_scripts.lib import get_work_directory


def test_map_job(tmpdir):
    from toil_scripts.lib.jobs import map_job
    work_dir = get_work_directory()
    options = Job.Runner.getDefaultOptions(os.path.join(work_dir, 'test_store'))
    options.workDir = work_dir
    samples = [x for x in xrange(200)]
    j = Job.wrapJobFn(map_job, _test_batch, samples, 'a', 'b', 'c', disk='1K')
    Job.Runner.startToil(j, options)


def _test_batch(job, sample, a, b, c):
    assert str(sample).isdigit()
    assert a == 'a'
    assert b == 'b'
    assert c == 'c'
