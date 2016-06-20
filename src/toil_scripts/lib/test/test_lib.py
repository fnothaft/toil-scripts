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
def test_flatten():
    from toil_scripts.lib import flatten
    x = [(1, 2), (3, 4, (5, 6))]
    y = (1, (2, (3, 4, (5))))
    assert flatten(x) == [1, 2, 3, 4, 5, 6]
    assert flatten(y) == [1, 2, 3, 4, 5]


def test_partitions():
    from toil_scripts.lib import partitions
    x = [z for z in xrange(100)]
    assert len(list(partitions(x, 10))) == 10
    assert len(list(partitions(x, 20))) == 5
    assert len(list(partitions(x, 100))) == 1
    assert list(partitions([], 10)) == []
