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
from toil_scripts.lib import partitions


def map_job(job, func, inputs, *args):
    """
    Spawns a tree of jobs to avoid overloading the number of jobs spawned by a single parent.
    This function is appropriate to use when batching samples greater than 1,000.

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param function func: Function to spawn dynamically, passes one sample as first argument
    :param list inputs: Array of samples to be batched
    :param list args: any arguments to be passed to the function
    """
    # num_partitions isn't exposed as an argument in order to be transparent to the user.
    # The value for num_partitions is a tested value
    num_partitions = 100
    partition_size = len(inputs) / num_partitions
    if partition_size > 1:
        for partition in partitions(inputs, partition_size):
            job.addChildJobFn(map_job, func, partition, *args)
    else:
        for sample in inputs:
            job.addChildJobFn(func, sample, *args)
