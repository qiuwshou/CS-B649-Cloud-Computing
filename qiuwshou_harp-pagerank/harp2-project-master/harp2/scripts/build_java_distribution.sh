#
# Copyright 2014 Indiana University
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

#######################################################
# In case of errors, you can use this scritp to kill  #
# all java processes in the nodes listed.             #
#######################################################
#!/bin/bash

if [ $# -ne 3 ]; then
    echo Usage: command [src folder] [package name] [dest folder]
    exit -1
fi

echo cd $1
echo tar zcf $3/$2.tar.gz $2/

cd $1
tar zcf $3/$2.tar.gz $2/

